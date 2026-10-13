namespace Orb;

public sealed class OrbStrategy
{
    public const decimal EsPointValue = 50m;
    public const decimal TickSize = 0.25m;
    public const decimal MaxInitialRisk = 600m;

    public OpeningRange? GetOpeningRange(IEnumerable<Candle> candles, DateOnly tradingDate)
    {
        var rangeBars = candles.Where(candle =>
        {
            var time = MarketTime.ToEastern(candle.T);
            return DateOnly.FromDateTime(time.Date) == tradingDate &&
                   time.TimeOfDay >= new TimeSpan(9, 30, 0) &&
                   time.TimeOfDay < new TimeSpan(9, 45, 0);
        }).ToArray();

        return rangeBars.Length == 15
            ? new OpeningRange { High = rangeBars.Max(candle => candle.H), Low = rangeBars.Min(candle => candle.L) }
            : null;
    }

    public (decimal? Intraday, decimal? PreviousDay) GetReferenceLevels(IEnumerable<Candle> candles, DateOnly tradingDate)
    {
        var allBars = candles.ToArray();
        var premarketStart = MarketTime.ToEastern(MarketTime.ToUtc(tradingDate.AddDays(-1), new TimeOnly(18, 0)));
        var marketOpen = MarketTime.ToEastern(MarketTime.ToUtc(tradingDate, new TimeOnly(9, 30)));
        var intradayBars = allBars.Where(candle =>
        {
            var time = MarketTime.ToEastern(candle.T);
            return time >= premarketStart && time < marketOpen;
        }).ToArray();

        var previousRth = allBars
            .Select(candle => (Candle: candle, Time: MarketTime.ToEastern(candle.T)))
            .Where(item => DateOnly.FromDateTime(item.Time.Date) < tradingDate &&
                           item.Time.TimeOfDay >= new TimeSpan(9, 30, 0) &&
                           item.Time.TimeOfDay < new TimeSpan(16, 0, 0))
            .GroupBy(item => DateOnly.FromDateTime(item.Time.Date))
            .OrderByDescending(group => group.Key)
            .FirstOrDefault();

        return (
            HasCompleteMinuteCoverage(intradayBars, premarketStart, marketOpen)
                ? intradayBars.Max(candle => candle.H)
                : null,
            previousRth is not null && HasCompleteMinuteCoverage(
                previousRth.Select(item => item.Candle),
                MarketTime.ToEastern(MarketTime.ToUtc(previousRth.Key, new TimeOnly(9, 30))),
                MarketTime.ToEastern(MarketTime.ToUtc(previousRth.Key, new TimeOnly(16, 0))))
                ? previousRth.Max(item => item.Candle.H)
                : null);
    }

    public (decimal? Intraday, decimal? PreviousDay) GetShortReferenceLevels(IEnumerable<Candle> candles, DateOnly tradingDate)
    {
        var allBars = candles.ToArray();
        var premarketStart = MarketTime.ToEastern(MarketTime.ToUtc(tradingDate.AddDays(-1), new TimeOnly(18, 0)));
        var marketOpen = MarketTime.ToEastern(MarketTime.ToUtc(tradingDate, new TimeOnly(9, 30)));
        var intradayBars = allBars.Where(candle =>
        {
            var time = MarketTime.ToEastern(candle.T);
            return time >= premarketStart && time < marketOpen;
        }).ToArray();

        var previousRth = allBars
            .Select(candle => (Candle: candle, Time: MarketTime.ToEastern(candle.T)))
            .Where(item => DateOnly.FromDateTime(item.Time.Date) < tradingDate &&
                           item.Time.TimeOfDay >= new TimeSpan(9, 30, 0) &&
                           item.Time.TimeOfDay < new TimeSpan(16, 0, 0))
            .GroupBy(item => DateOnly.FromDateTime(item.Time.Date))
            .OrderByDescending(group => group.Key)
            .FirstOrDefault();

        return (
            HasCompleteMinuteCoverage(intradayBars, premarketStart, marketOpen)
                ? intradayBars.Min(candle => candle.L)
                : null,
            previousRth is not null && HasCompleteMinuteCoverage(
                previousRth.Select(item => item.Candle),
                MarketTime.ToEastern(MarketTime.ToUtc(previousRth.Key, new TimeOnly(9, 30))),
                MarketTime.ToEastern(MarketTime.ToUtc(previousRth.Key, new TimeOnly(16, 0))))
                ? previousRth.Min(item => item.Candle.L)
                : null);
    }

    public TradeDirection? GetBreakout(Candle candle, OpeningRange openingRange) =>
        candle.C > openingRange.High ? TradeDirection.Long :
        candle.C < openingRange.Low ? TradeDirection.Short : null;

    public int GetQuantity(decimal entryPrice, decimal stopPrice, decimal allowance)
    {
        var riskPerContract = Math.Abs(entryPrice - stopPrice) * EsPointValue + allowance;
        return riskPerContract <= 0 ? 0 : (int)Math.Floor(MaxInitialRisk / riskPerContract);
    }

    public TradeState CreateTrade(
        TradeDirection direction,
        string contractId,
        int quantity,
        decimal entryPrice,
        decimal initialStop,
        decimal? intradayLevel,
        decimal? previousDayLevel)
    {
        var references = new List<ReferenceLevel>();
        AddReference(references, "intraday", intradayLevel, direction, entryPrice);
        AddReference(references, "previous-day", previousDayLevel, direction, entryPrice);
        return new TradeState
        {
            Direction = direction,
            ContractId = contractId,
            Quantity = quantity,
            EntryPrice = entryPrice,
            InitialStop = initialStop,
            PeakProfit = 0,
            ReferenceLevels = references
        };
    }

    public decimal CalculateInitialStop(
        TradeDirection direction,
        decimal entryPrice,
        OpeningRange openingRange,
        IReadOnlyList<Candle> candles)
    {
        var defaultStop = direction == TradeDirection.Long
            ? openingRange.High - (openingRange.Width * .75m)
            : openingRange.Low + (openingRange.Width * .75m);
        var fvgStop = FindInitialFvgStop(direction, entryPrice, openingRange, candles);
        return RoundStop(direction, fvgStop is not null && IsMoreProtective(direction, fvgStop.Value, defaultStop)
            ? fvgStop.Value
            : defaultStop);
    }

    public StageAction? GetNextStageAction(
        TradeState trade,
        decimal marketPrice,
        OpeningRange openingRange,
        IReadOnlyList<Candle> candles)
    {
        var profit = GetProfit(trade, marketPrice);
        trade.PeakProfit = Math.Max(trade.PeakProfit, profit);

        if (trade.CompletedStage == 2)
        {
            var reference = GetReachedUnusedReference(trade, marketPrice);
            if (profit < 250m && reference is null)
            {
                return null;
            }

            return new StageAction
            {
                Stage = 3,
                Reason = BuildReason("stage-3", profit >= 250m, reference),
                ReferencePrice = reference?.Price,
                DesiredStop = GetStage3Stop(trade, marketPrice, openingRange, candles)
            };
        }

        if (trade.CompletedStage == 3)
        {
            var reference = GetReachedUnusedReference(trade, marketPrice);
            if (profit < 500m && reference is null)
            {
                return null;
            }

            decimal? profitTarget = profit >= 500m ? ProfitToPrice(trade, 250m) : null;
            var referenceTarget = reference is null
                ? null
                : GetReferenceFvgStop(trade, marketPrice, openingRange, reference.Price, candles);

            return new StageAction
            {
                Stage = 4,
                Reason = BuildReason("stage-4", profit >= 500m, reference),
                ReferencePrice = reference?.Price,
                DesiredStop = MoreProtective(trade.Direction, profitTarget, referenceTarget)
            };
        }

        if (trade.CompletedStage == 4)
        {
            return GetStage5Action(trade);
        }

        return null;
    }

    public void CompleteStage(TradeState trade, StageAction action)
    {
        if (action.ReferencePrice is decimal level && !trade.UsedReferencePrices.Contains(level))
        {
            trade.UsedReferencePrices.Add(level);
        }

        if (action.Stage == 3)
        {
            trade.CompletedStage = 3;
        }
        else if (action.Stage == 4)
        {
            trade.CompletedStage = 4;
            trade.Stage4PeakProfit = trade.PeakProfit;
            trade.Stage4SecuredProfit = trade.ConfirmedStop is decimal stop
                ? Math.Max(0, GetProfit(trade, stop))
                : 0;
        }
    }

    public decimal GetProfit(TradeState trade, decimal price) =>
        (trade.Direction == TradeDirection.Long ? price - trade.EntryPrice : trade.EntryPrice - price)
        * EsPointValue * trade.Quantity;

    public bool IsValidProtectiveStop(TradeState trade, decimal stop, decimal marketPrice) =>
        trade.Direction == TradeDirection.Long ? stop < marketPrice : stop > marketPrice;

    public bool IsMoreProtective(TradeDirection direction, decimal candidate, decimal current) =>
        direction == TradeDirection.Long ? candidate > current : candidate < current;

    private StageAction? GetStage5Action(TradeState trade)
    {
        if (trade.Stage4PeakProfit is not decimal stage4Peak || trade.Stage4SecuredProfit is not decimal secured)
        {
            return null;
        }

        decimal? desiredProfit = null;
        if (trade.PeakProfit >= 1000m)
        {
            desiredProfit = trade.PeakProfit - 250m;
        }
        else
        {
            var increments = (int)Math.Floor((trade.PeakProfit - stage4Peak) / 250m);
            if (increments > 0)
            {
                desiredProfit = secured + (increments * 250m);
            }
        }

        return desiredProfit is null
            ? null
            : new StageAction
            {
                Stage = 5,
                Reason = trade.PeakProfit >= 1000m ? "stage-5-continuous-trail" : "stage-5-incremental-trail",
                DesiredStop = RoundStop(trade.Direction, ProfitToPrice(trade, desiredProfit.Value))
            };
    }

    private decimal? GetStage3Stop(
        TradeState trade,
        decimal marketPrice,
        OpeningRange openingRange,
        IReadOnlyList<Candle> candles)
    {
        var defaultStop = trade.Direction == TradeDirection.Long
            ? openingRange.High - (openingRange.Width * .25m)
            : openingRange.Low + (openingRange.Width * .25m);
        var fvgStop = FindStage3FvgStop(trade.Direction, marketPrice, openingRange, candles);
        return MoreProtective(trade.Direction, RoundStop(trade.Direction, defaultStop), fvgStop);
    }

    private decimal? GetReferenceFvgStop(
        TradeState trade,
        decimal marketPrice,
        OpeningRange openingRange,
        decimal reference,
        IReadOnlyList<Candle> candles)
    {
        var boundary = trade.Direction == TradeDirection.Long ? openingRange.High : openingRange.Low;
        var candidates = GetFairValueGaps(candles)
            .Where(gap => gap.Bullish == (trade.Direction == TradeDirection.Long))
            .Where(gap => IsUnfilled(gap, candles))
            .Where(gap => trade.Direction == TradeDirection.Long
                ? gap.Lower >= boundary && gap.Upper <= reference
                : gap.Upper <= boundary && gap.Lower >= reference)
            .Select(gap => new
            {
                Gap = gap,
                Stop = RoundStop(trade.Direction, trade.Direction == TradeDirection.Long ? gap.Lower - TickSize : gap.Upper + TickSize)
            })
            .Where(candidate => GetProfit(trade, candidate.Stop) > 0 &&
                                IsValidProtectiveStop(trade, candidate.Stop, marketPrice) &&
                                IsMoreProtective(
                                    trade.Direction,
                                    candidate.Stop,
                                    trade.ConfirmedStop ?? trade.InitialStop))
            .OrderByDescending(candidate => candidate.Gap.Width)
            .ToArray();

        return candidates.FirstOrDefault()?.Stop;
    }

    private decimal? FindInitialFvgStop(
        TradeDirection direction,
        decimal entryPrice,
        OpeningRange openingRange,
        IReadOnlyList<Candle> candles)
    {
        var candidates = GetFairValueGaps(candles)
            .Where(gap => gap.Bullish == (direction == TradeDirection.Long))
            .Select(gap => new
            {
                Stop = RoundStop(direction, direction == TradeDirection.Long ? gap.Lower - TickSize : gap.Upper + TickSize),
                Qualifies = direction == TradeDirection.Long ? gap.Lower > openingRange.Midpoint : gap.Upper < openingRange.Midpoint
            })
            .Where(item => item.Qualifies && (direction == TradeDirection.Long ? item.Stop < entryPrice : item.Stop > entryPrice))
            .Select(item => item.Stop);

        var stops = candidates.ToArray();
        return stops.Length == 0 ? null : direction == TradeDirection.Long ? stops.Max() : stops.Min();
    }

    private decimal? FindStage3FvgStop(
        TradeDirection direction,
        decimal marketPrice,
        OpeningRange openingRange,
        IReadOnlyList<Candle> candles)
    {
        var nearBoundary = direction == TradeDirection.Long
            ? openingRange.High - (openingRange.Width * .25m)
            : openingRange.Low + (openingRange.Width * .25m);

        var candidates = GetFairValueGaps(candles)
            .Where(gap => gap.Bullish == (direction == TradeDirection.Long))
            .Where(gap => IsUnfilled(gap, candles))
            .Select(gap => new
            {
                Stop = RoundStop(direction, direction == TradeDirection.Long ? gap.Lower - TickSize : gap.Upper + TickSize),
                NearBoundary = direction == TradeDirection.Long ? gap.Lower >= nearBoundary : gap.Upper <= nearBoundary
            })
            .Where(item => item.NearBoundary && (direction == TradeDirection.Long ? item.Stop < marketPrice : item.Stop > marketPrice))
            .Select(item => item.Stop);

        var stops = candidates.ToArray();
        return stops.Length == 0 ? null : direction == TradeDirection.Long ? stops.Max() : stops.Min();
    }

    private static IEnumerable<FairValueGap> GetFairValueGaps(IReadOnlyList<Candle> candles)
    {
        for (var index = 2; index < candles.Count; index++)
        {
            var first = candles[index - 2];
            var third = candles[index];
            if (third.L > first.H)
            {
                yield return new FairValueGap { Bullish = true, Lower = first.H, Upper = third.L, CreatedAt = third.T };
            }

            else if (third.H < first.L)
            {
                yield return new FairValueGap { Bullish = false, Lower = third.H, Upper = first.L, CreatedAt = third.T };
            }
        }
    }

    private static bool HasCompleteMinuteCoverage(
        IEnumerable<Candle> candles,
        DateTimeOffset start,
        DateTimeOffset end)
    {
        var minutes = candles
            .Select(candle => MarketTime.ToEastern(candle.T))
            .Select(time => new DateTimeOffset(time.Year, time.Month, time.Day, time.Hour, time.Minute, 0, time.Offset))
            .Distinct()
            .OrderBy(time => time)
            .ToArray();

        var expectedMinutes = (int)(end - start).TotalMinutes;
        return minutes.Length == expectedMinutes &&
               minutes.Length > 0 &&
               minutes[0] == start &&
               minutes[^1] == end.AddMinutes(-1) &&
               minutes.Zip(minutes.Skip(1), (first, second) => second - first).All(span => span == TimeSpan.FromMinutes(1));
    }

    private static bool IsUnfilled(FairValueGap gap, IReadOnlyList<Candle> candles) =>
        gap.Bullish
            ? !candles.Where(candle => candle.T > gap.CreatedAt).Any(candle => candle.L <= gap.Lower)
            : !candles.Where(candle => candle.T > gap.CreatedAt).Any(candle => candle.H >= gap.Upper);

    private static void AddReference(
        List<ReferenceLevel> levels,
        string name,
        decimal? candidate,
        TradeDirection direction,
        decimal entryPrice)
    {
        if (candidate is not decimal price ||
            (direction == TradeDirection.Long ? price <= entryPrice : price >= entryPrice) ||
            levels.Any(level => level.Price == price))
        {
            return;
        }

        levels.Add(new ReferenceLevel { Name = name, Price = price });
    }

    private static ReferenceLevel? GetReachedUnusedReference(TradeState trade, decimal marketPrice) =>
        trade.ReferenceLevels.FirstOrDefault(level =>
            !trade.UsedReferencePrices.Contains(level.Price) &&
            (trade.Direction == TradeDirection.Long ? marketPrice >= level.Price : marketPrice <= level.Price));

    private static string BuildReason(string stage, bool profitReached, ReferenceLevel? reference) =>
        $"{stage}:{(profitReached ? "profit-threshold" : string.Empty)}{(profitReached && reference is not null ? "+" : string.Empty)}{reference?.Name ?? string.Empty}";

    private static decimal? MoreProtective(TradeDirection direction, decimal? first, decimal? second) =>
        first is null ? second :
        second is null ? first :
        direction == TradeDirection.Long ? Math.Max(first.Value, second.Value) : Math.Min(first.Value, second.Value);

    private static decimal ProfitToPrice(TradeState trade, decimal profit) =>
        trade.Direction == TradeDirection.Long
            ? trade.EntryPrice + (profit / (EsPointValue * trade.Quantity))
            : trade.EntryPrice - (profit / (EsPointValue * trade.Quantity));

    private static decimal RoundStop(TradeDirection direction, decimal price) =>
        direction == TradeDirection.Long
            ? Math.Floor(price / TickSize) * TickSize
            : Math.Ceiling(price / TickSize) * TickSize;
}

using Microsoft.Extensions.Options;

namespace Orb;

public sealed class Worker(
    TopstepClient topstep,
    MarketDataFeed marketData,
    OrbStrategy strategy,
    SessionStore sessionStore,
    TradeJournal journal,
    IOptions<TopstepOptions> options,
    ILogger<Worker> logger) : BackgroundService
{
    private readonly SemaphoreSlim _stateLock = new(1, 1);
    private OrbSessionState? _session;
    private TopstepAccount? _account;
    private TopstepContract? _contract;
    private OpeningRange? _openingRange;
    private IReadOnlyList<Candle> _candles = [];
    private DateTimeOffset? _lastHistoryMinute;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        marketData.QuoteReceived += ProcessQuoteAsync;
        await journal.WriteAsync("ORB worker started.", stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "ORB worker cycle failed.");
                await journal.WriteAsync($"worker-error:{exception.GetType().Name}:{exception.Message}", stoppingToken);
            }

            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }

    private async Task RunOnceAsync(CancellationToken cancellationToken)
    {
        var easternNow = MarketTime.ToEastern(DateTimeOffset.UtcNow);
        var tradingDate = DateOnly.FromDateTime(easternNow.Date);
        if (easternNow.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday)
        {
            return;
        }

        await _stateLock.WaitAsync(cancellationToken);
        try
        {
            if (_session?.TradingDate != tradingDate)
            {
                _session = await sessionStore.LoadAsync(tradingDate, cancellationToken);
                _openingRange = null;
                _candles = [];
                _lastHistoryMinute = null;
                await journal.WriteAsync($"trading-date:{tradingDate:yyyy-MM-dd}", cancellationToken);
            }

            _account ??= await topstep.GetTradingAccountAsync(cancellationToken);
            _contract ??= await topstep.GetActiveEsContractAsync(cancellationToken);
            await marketData.EnsureStartedAsync(_contract.Id, cancellationToken);

            if (easternNow.TimeOfDay >= new TimeSpan(16, 0, 0))
            {
                await CloseForSessionEndAsync(cancellationToken);
                return;
            }

            if (easternNow.TimeOfDay < new TimeSpan(9, 45, 0) || !IsNewHistoryMinute(easternNow))
            {
                return;
            }

            await RefreshBarsAndProcessAsync(tradingDate, easternNow, cancellationToken);
        }
        finally
        {
            _stateLock.Release();
        }
    }

    private bool IsNewHistoryMinute(DateTimeOffset easternNow)
    {
        var minute = new DateTimeOffset(
            easternNow.Year,
            easternNow.Month,
            easternNow.Day,
            easternNow.Hour,
            easternNow.Minute,
            0,
            easternNow.Offset);
        return easternNow.Second >= 2 && _lastHistoryMinute != minute;
    }

    private async Task RefreshBarsAndProcessAsync(
        DateOnly tradingDate,
        DateTimeOffset easternNow,
        CancellationToken cancellationToken)
    {
        if (_contract is null || _account is null || _session is null)
        {
            return;
        }

        var start = MarketTime.ToUtc(tradingDate.AddDays(-7), TimeOnly.MinValue);
        _candles = await topstep.GetMinuteBarsAsync(_contract.Id, start, easternNow.ToUniversalTime(), cancellationToken);
        _lastHistoryMinute = new DateTimeOffset(
            easternNow.Year,
            easternNow.Month,
            easternNow.Day,
            easternNow.Hour,
            easternNow.Minute,
            0,
            easternNow.Offset);
        _openingRange ??= strategy.GetOpeningRange(_candles, tradingDate);
        if (_openingRange is null)
        {
            await journal.WriteAsync("opening-range-incomplete", cancellationToken);
            return;
        }

        await ResolvePositionStateAsync(cancellationToken);
        if (_session.Trade is not null)
        {
            if (marketData.LastQuote is { LastPrice: > 0 } quote)
            {
                await ProcessTradeAsync(quote.LastPrice, cancellationToken);
            }

            return;
        }

        if (_session.TradeSubmitted)
        {
            return;
        }

        var closedBars = _candles
            .Where(candle =>
            {
                var time = MarketTime.ToEastern(candle.T);
                return DateOnly.FromDateTime(time.Date) == tradingDate &&
                       time.TimeOfDay >= new TimeSpan(9, 45, 0);
            })
            .Where(candle => _session.LastEntryCandle is null || candle.T > _session.LastEntryCandle)
            .OrderBy(candle => candle.T)
            .ToArray();

        foreach (var candle in closedBars)
        {
            _session.LastEntryCandle = candle.T;
            var direction = strategy.GetBreakout(candle, _openingRange);
            if (direction is not null)
            {
                await SubmitEntryAsync(direction.Value, candle, tradingDate, cancellationToken);
                break;
            }
        }

        await sessionStore.SaveAsync(_session, cancellationToken);
    }

    private async Task ResolvePositionStateAsync(CancellationToken cancellationToken)
    {
        if (_account is null || _contract is null || _session is null)
        {
            return;
        }

        var position = (await topstep.GetOpenPositionsAsync(_account.Id, cancellationToken))
            .SingleOrDefault(item => item.ContractId == _contract.Id);

        if (_session.Trade is null)
        {
            if (position is not null)
            {
                if (_session.PendingEntryDirection is TradeDirection pendingDirection)
                {
                    await ActivateFilledEntryAsync(pendingDirection, cancellationToken);
                    return;
                }

                _session.TradeSubmitted = true;
                await journal.WriteAsync($"open-unmanaged-position:size={position.Size}; entry disabled for today", cancellationToken);
                await sessionStore.SaveAsync(_session, cancellationToken);
            }

            return;
        }

        if (position is null)
        {
            await journal.WriteAsync($"position-closed:entry={_session.Trade.EntryPrice}; peak-profit={_session.Trade.PeakProfit}", cancellationToken);
            _session.Trade = null;
            await sessionStore.SaveAsync(_session, cancellationToken);
        }
    }

    private async Task SubmitEntryAsync(
        TradeDirection direction,
        Candle breakoutCandle,
        DateOnly tradingDate,
        CancellationToken cancellationToken)
    {
        if (_session is null || _contract is null || _account is null || _openingRange is null)
        {
            return;
        }

        var expectedEntry = marketData.LastQuote is { LastPrice: > 0 } quote ? quote.LastPrice : breakoutCandle.C;
        var initialStop = strategy.CalculateInitialStop(direction, expectedEntry, _openingRange, _candles);
        var quantity = strategy.GetQuantity(expectedEntry, initialStop, options.Value.CommissionAndSlippagePerContract);
        if (quantity < 1)
        {
            await journal.WriteAsync($"entry-rejected:risk-over-600; direction={direction}; expected-entry={expectedEntry}; stop={initialStop}", cancellationToken);
            _session.TradeSubmitted = true;
            return;
        }

        _session.TradeSubmitted = true;
        _session.PendingEntryDirection = direction;
        await sessionStore.SaveAsync(_session, cancellationToken);
        var tag = $"orb-{tradingDate:yyyyMMdd}-entry";
        try
        {
            var order = await topstep.PlaceOrderAsync(
                _account.Id,
                _contract.Id,
                type: 2,
                side: direction == TradeDirection.Long ? 0 : 1,
                size: quantity,
                stopPrice: null,
                customTag: tag,
                cancellationToken);
            await journal.WriteAsync($"entry-submitted:order={order.OrderId}; direction={direction}; size={quantity}; signal-close={breakoutCandle.C}", cancellationToken);

            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            await ActivateFilledEntryAsync(direction, cancellationToken);
        }
        catch (Exception exception)
        {
            await journal.WriteAsync($"entry-submit-failed:{exception.GetType().Name}:{exception.Message}; entry disabled for today", cancellationToken);
        }
    }

    private async Task ActivateFilledEntryAsync(TradeDirection direction, CancellationToken cancellationToken)
    {
        if (_session is null || _contract is null || _account is null || _openingRange is null)
        {
            return;
        }

        var position = (await topstep.GetOpenPositionsAsync(_account.Id, cancellationToken))
            .SingleOrDefault(item => item.ContractId == _contract.Id);
        if (position is null)
        {
            await journal.WriteAsync("entry-pending:no-position-confirmation", cancellationToken);
            return;
        }

        if (marketData.LastQuote is not { LastPrice: > 0 } quote)
        {
            await ClosePositionAsync("entry-filled-without-current-quote", cancellationToken);
            return;
        }

        var directionFromBroker = position.Type == 1 ? TradeDirection.Long : TradeDirection.Short;
        if (directionFromBroker != direction)
        {
            await ClosePositionAsync("entry-direction-mismatch", cancellationToken);
            return;
        }

        var initialStop = strategy.CalculateInitialStop(direction, position.AveragePrice, _openingRange, _candles);
        if (strategy.GetQuantity(position.AveragePrice, initialStop, options.Value.CommissionAndSlippagePerContract) < position.Size)
        {
            await ClosePositionAsync("filled-risk-exceeds-600", cancellationToken);
            return;
        }

        var date = _session.TradingDate;
        var references = direction == TradeDirection.Long
            ? strategy.GetReferenceLevels(_candles, date)
            : strategy.GetShortReferenceLevels(_candles, date);
        _session.Trade = strategy.CreateTrade(
            direction,
            _contract.Id,
            position.Size,
            position.AveragePrice,
            initialStop,
            references.Intraday,
            references.PreviousDay);
        _session.PendingEntryDirection = null;

        if (!await EnsureProtectiveStopAsync(initialStop, "initial-stop", quote.LastPrice, cancellationToken))
        {
            return;
        }

        await sessionStore.SaveAsync(_session, cancellationToken);
        await journal.WriteAsync(
            $"entry-filled:direction={direction}; size={position.Size}; actual-entry={position.AveragePrice}; initial-stop={_session.Trade.ConfirmedStop}",
            cancellationToken);
    }

    private async Task ProcessQuoteAsync(Quote quote)
    {
        if (quote.LastPrice <= 0)
        {
            return;
        }

        await _stateLock.WaitAsync();
        try
        {
            if (_session?.Trade is not null && _openingRange is not null)
            {
                await ProcessTradeAsync(quote.LastPrice, CancellationToken.None);
            }
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Live quote processing failed.");
            await journal.WriteAsync($"quote-processing-failed:{exception.GetType().Name}:{exception.Message}", CancellationToken.None);
        }
        finally
        {
            _stateLock.Release();
        }
    }

    private async Task ProcessTradeAsync(decimal marketPrice, CancellationToken cancellationToken)
    {
        if (_session?.Trade is not TradeState trade || _openingRange is null)
        {
            return;
        }

        if (trade.ConfirmedStop is decimal existingStop && !strategy.IsValidProtectiveStop(trade, existingStop, marketPrice))
        {
            await ClosePositionAsync("existing-stop-crossed-before-confirmation", cancellationToken);
            return;
        }

        for (var step = 0; step < 3 && _session.Trade is not null; step++)
        {
            trade = _session.Trade;
            var action = strategy.GetNextStageAction(trade, marketPrice, _openingRange, _candles);
            if (action is null)
            {
                break;
            }

            if (action.DesiredStop is decimal desiredStop &&
                !await EnsureProtectiveStopAsync(desiredStop, action.Reason, marketPrice, cancellationToken))
            {
                return;
            }
            else if (action.DesiredStop is null && !await ConfirmExistingStopAsync(trade, cancellationToken))
            {
                await ClosePositionAsync($"unconfirmed-stop-before-{action.Reason}", cancellationToken);
                return;
            }

            if (action.Stage is 3 or 4)
            {
                strategy.CompleteStage(trade, action);
                await journal.WriteAsync(
                    $"{action.Reason}; market={marketPrice}; confirmed-stop={trade.ConfirmedStop}; peak-profit={trade.PeakProfit}",
                    cancellationToken);
            }
            else
            {
                await journal.WriteAsync(
                    $"{action.Reason}; market={marketPrice}; confirmed-stop={trade.ConfirmedStop}; peak-profit={trade.PeakProfit}",
                    cancellationToken);
                break;
            }

            await sessionStore.SaveAsync(_session, cancellationToken);
        }

        if (_session is not null)
        {
            await sessionStore.SaveAsync(_session, cancellationToken);
        }
    }

    private async Task<bool> EnsureProtectiveStopAsync(
        decimal requestedStop,
        string reason,
        decimal marketPrice,
        CancellationToken cancellationToken)
    {
        if (_session?.Trade is not TradeState trade || _account is null || _contract is null)
        {
            return false;
        }

        requestedStop = trade.Direction == TradeDirection.Long
            ? Math.Floor(requestedStop / OrbStrategy.TickSize) * OrbStrategy.TickSize
            : Math.Ceiling(requestedStop / OrbStrategy.TickSize) * OrbStrategy.TickSize;

        if (trade.ConfirmedStop is decimal currentStop &&
            !strategy.IsMoreProtective(trade.Direction, requestedStop, currentStop))
        {
            return await ConfirmExistingStopAsync(trade, cancellationToken);
        }

        if (!strategy.IsValidProtectiveStop(trade, requestedStop, marketPrice))
        {
            await ClosePositionAsync($"invalid-stop-before-{reason}", cancellationToken);
            return false;
        }

        try
        {
            if (trade.StopOrderId is null)
            {
                var response = await topstep.PlaceOrderAsync(
                    _account.Id,
                    _contract.Id,
                    type: 4,
                    side: trade.Direction == TradeDirection.Long ? 1 : 0,
                    size: trade.Quantity,
                    stopPrice: requestedStop,
                    customTag: $"orb-{_session.TradingDate:yyyyMMdd}-stop",
                    cancellationToken);
                trade.StopOrderId = response.OrderId;
            }
            else
            {
                await topstep.ModifyOrderAsync(_account.Id, trade.StopOrderId.Value, trade.Quantity, requestedStop, cancellationToken);
            }

            if (await ConfirmStopAsync(trade, requestedStop, cancellationToken))
            {
                await journal.WriteAsync($"stop-confirmed:{reason}; order={trade.StopOrderId}; price={requestedStop}", cancellationToken);
                return true;
            }
        }
        catch (Exception exception)
        {
            await journal.WriteAsync($"stop-update-failed:{reason}; {exception.GetType().Name}:{exception.Message}", cancellationToken);
        }

        if (await ConfirmExistingStopAsync(trade, cancellationToken))
        {
            await journal.WriteAsync($"stop-update-deferred:{reason}; existing-stop={trade.ConfirmedStop}", cancellationToken);
            return false;
        }

        await ClosePositionAsync($"unconfirmed-stop-after-{reason}", cancellationToken);
        return false;
    }

    private async Task<bool> ConfirmStopAsync(TradeState trade, decimal expectedStop, CancellationToken cancellationToken)
    {
        if (_account is null)
        {
            return false;
        }

        for (var attempt = 0; attempt < 3; attempt++)
        {
            var order = (await topstep.GetOpenOrdersAsync(_account.Id, cancellationToken))
                .SingleOrDefault(item => item.Id == trade.StopOrderId &&
                                         item.Type == 4 &&
                                         item.StopPrice == expectedStop);
            if (order is not null)
            {
                trade.ConfirmedStop = order.StopPrice;
                return true;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }

        return false;
    }

    private async Task<bool> ConfirmExistingStopAsync(TradeState trade, CancellationToken cancellationToken)
    {
        if (_account is null || trade.StopOrderId is null)
        {
            return false;
        }

        var order = (await topstep.GetOpenOrdersAsync(_account.Id, cancellationToken))
            .SingleOrDefault(item => item.Id == trade.StopOrderId && item.Type == 4 && item.StopPrice is not null);
        if (order?.StopPrice is not decimal stop)
        {
            return false;
        }

        trade.ConfirmedStop = stop;
        return true;
    }

    private async Task CloseForSessionEndAsync(CancellationToken cancellationToken)
    {
        if (_session?.Trade is not null)
        {
            await ClosePositionAsync("4pm-et-session-close", cancellationToken);
        }
    }

    private async Task ClosePositionAsync(string reason, CancellationToken cancellationToken)
    {
        if (_account is null || _contract is null)
        {
            return;
        }

        try
        {
            await topstep.ClosePositionAsync(_account.Id, _contract.Id, cancellationToken);
            await journal.WriteAsync($"position-close-submitted:{reason}", cancellationToken);
        }
        catch (Exception exception)
        {
            await journal.WriteAsync($"position-close-failed:{reason}; {exception.GetType().Name}:{exception.Message}", cancellationToken);
        }
    }
}

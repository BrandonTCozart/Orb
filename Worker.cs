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

            if (easternNow.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday)
            {
                return;
            }

            if (easternNow.TimeOfDay >= new TimeSpan(16, 0, 0))
            {
                await CloseForSessionEndAsync(cancellationToken);
                return;
            }

            if (easternNow.TimeOfDay < new TimeSpan(9, 45, 0))
            {
                await CloseOpenContractPositionAsync("position-outside-trading-window", cancellationToken);
                return;
            }

            if (!IsNewHistoryMinute(easternNow))
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
        await ResolvePositionStateAsync(cancellationToken);
        if (_openingRange is null)
        {
            await journal.WriteAsync("opening-range-incomplete", cancellationToken);
            return;
        }
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

        var unseenCompletedCandles = _candles
            .Where(candle =>
            {
                var time = MarketTime.ToEastern(candle.T);
                return DateOnly.FromDateTime(time.Date) == tradingDate &&
                       time.TimeOfDay >= new TimeSpan(9, 45, 0) &&
                       time <= easternNow.AddMinutes(-1);
            })
            .Where(candle => _session.LastEntryCandle is null || candle.T > _session.LastEntryCandle)
            .OrderBy(candle => candle.T)
            .ToArray();

        foreach (var candle in unseenCompletedCandles)
        {
            _session.LastEntryCandle = candle.T;
            if (easternNow - MarketTime.ToEastern(candle.T) > TimeSpan.FromMinutes(2))
            {
                await journal.WriteAsync($"entry-skipped:stale-breakout-candle={candle.T:O}", cancellationToken);
                continue;
            }

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

        var positions = (await topstep.GetOpenPositionsAsync(_account.Id, cancellationToken))
            .Where(item => item.ContractId == _contract.Id)
            .ToArray();
        var openOrders = (await topstep.GetOpenOrdersAsync(_account.Id, cancellationToken))
            .Where(order => order.ContractId == _contract.Id)
            .ToArray();
        if (positions.Length > 1)
        {
            await ClosePositionAsync("multiple-open-contract-positions", cancellationToken);
            return;
        }

        var position = positions.SingleOrDefault();

        if (_session.Trade is null)
        {
            if (position is not null)
            {
                if (_session.PendingEntryDirection is TradeDirection pendingDirection)
                {
                    await ActivateFilledEntryAsync(pendingDirection, cancellationToken);
                    return;
                }

                await ClosePositionAsync("open-unmanaged-position", cancellationToken);
            }

            await ReconcilePendingEntryAsync(openOrders, cancellationToken);
            await ReconcileExecutionsAsync(null, cancellationToken);
            return;
        }

        if (position is null)
        {
            await ReconcileExecutionsAsync(_session.Trade, cancellationToken);
            await journal.WriteAsync($"position-closed:entry={_session.Trade.EntryPrice}; peak-profit={_session.Trade.PeakProfit}", cancellationToken);
            _session.Trade = null;
            await sessionStore.SaveAsync(_session, cancellationToken);
            return;
        }

        if (!MatchesTradePosition(position, _session.Trade))
        {
            await ClosePositionAsync("saved-state-position-mismatch", cancellationToken);
            return;
        }

        if (!await ConfirmExistingStopAsync(_session.Trade, cancellationToken))
        {
            if (marketData.LastQuote is not { LastPrice: > 0 } recoveryQuote ||
                !await EnsureProtectiveStopAsync(
                    _session.Trade.InitialStop,
                    "recovery-initial-stop",
                    recoveryQuote.LastPrice,
                    cancellationToken))
            {
                await ClosePositionAsync("unprotected-recovered-position", cancellationToken);
            }

            return;
        }

        if (marketData.LastQuote is not { LastPrice: > 0 } quote)
        {
            await journal.WriteAsync("recovered-position:waiting-for-market-quote", cancellationToken);
            await sessionStore.SaveAsync(_session, cancellationToken);
            return;
        }

        if (!strategy.IsValidProtectiveStop(_session.Trade, _session.Trade.ConfirmedStop!.Value, quote.LastPrice))
        {
            await ClosePositionAsync("recovered-stop-crossed-before-confirmation", cancellationToken);
            return;
        }

        await ReconcileExecutionsAsync(_session.Trade, cancellationToken);
        await sessionStore.SaveAsync(_session, cancellationToken);
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
        var initialStop = strategy.CalculateInitialStop(
            direction,
            expectedEntry,
            _openingRange,
            _candles,
            options.Value.CommissionAndSlippagePerContract);

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
                size: OrbStrategy.EntryQuantity,
                stopPrice: null,
                customTag: tag,
                cancellationToken);
            _session.PendingEntryOrderId = order.OrderId;
            await sessionStore.SaveAsync(_session, cancellationToken);
            await journal.WriteAsync($"entry-submitted:order={order.OrderId}; direction={direction}; size={OrbStrategy.EntryQuantity}; signal-close={breakoutCandle.C}", cancellationToken);

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
        if (_session is null || _contract is null || _account is null)
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

        if (_openingRange is null)
        {
            await ClosePositionAsync("entry-filled-without-opening-range", cancellationToken);
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

        if (position.Size != OrbStrategy.EntryQuantity)
        {
            await ClosePositionAsync("entry-size-not-one-contract", cancellationToken);
            return;
        }

        var entryPrice = await GetActualEntryPriceAsync(direction, position, cancellationToken);
        var initialStop = strategy.CalculateInitialStop(
            direction,
            entryPrice,
            _openingRange,
            _candles,
            options.Value.CommissionAndSlippagePerContract);

        var date = _session.TradingDate;
        var references = direction == TradeDirection.Long
            ? strategy.GetReferenceLevels(_candles, date)
            : strategy.GetShortReferenceLevels(_candles, date);
        _session.Trade = strategy.CreateTrade(
            direction,
            _contract.Id,
            position.Size,
            entryPrice,
            initialStop,
            references.Intraday,
            references.PreviousDay);
        _session.PendingEntryDirection = null;
        _session.PendingEntryOrderId = null;

        if (!await EnsureProtectiveStopAsync(initialStop, "initial-stop", quote.LastPrice, cancellationToken))
        {
            return;
        }

        await sessionStore.SaveAsync(_session, cancellationToken);
        await journal.WriteAsync(
            $"entry-filled:direction={direction}; size={position.Size}; actual-entry={position.AveragePrice}; initial-stop={_session.Trade.ConfirmedStop}",
            cancellationToken);
    }

    private async Task ReconcilePendingEntryAsync(
        IReadOnlyList<TopstepOrder> openOrders,
        CancellationToken cancellationToken)
    {
        if (_session is null || _account is null || _contract is null ||
            _session.PendingEntryDirection is not TradeDirection direction)
        {
            return;
        }

        var tag = $"orb-{_session.TradingDate:yyyyMMdd}-entry";
        var pendingOrders = openOrders
            .Where(order => order.Type == 2 &&
                            (order.Id == _session.PendingEntryOrderId || order.CustomTag == tag))
            .ToArray();
        foreach (var order in pendingOrders)
        {
            await topstep.CancelOrderAsync(_account.Id, order.Id, cancellationToken);
            await journal.WriteAsync($"recovery-entry-cancel-submitted:order={order.Id}", cancellationToken);
        }

        if (pendingOrders.Length > 0)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
            var remainingPendingOrders = (await topstep.GetOpenOrdersAsync(_account.Id, cancellationToken))
                .Where(order => order.ContractId == _contract.Id &&
                                order.Type == 2 &&
                                (order.Id == _session.PendingEntryOrderId || order.CustomTag == tag))
                .ToArray();
            if (remainingPendingOrders.Length > 0)
            {
                await journal.WriteAsync(
                    $"recovery-entry-cancel-pending:order={remainingPendingOrders[0].Id}",
                    cancellationToken);
                return;
            }

            var position = (await topstep.GetOpenPositionsAsync(_account.Id, cancellationToken))
                .SingleOrDefault(item => item.ContractId == _contract.Id);
            if (position is not null)
            {
                await ActivateFilledEntryAsync(direction, cancellationToken);
                return;
            }
        }

        var orders = (await topstep.GetOrdersAsync(
                _account.Id,
                MarketTime.ToUtc(_session.TradingDate, TimeOnly.MinValue),
                DateTimeOffset.UtcNow,
                cancellationToken))
            .Where(order => order.ContractId == _contract.Id &&
                            (order.Id == _session.PendingEntryOrderId || order.CustomTag == tag))
            .OrderByDescending(order => order.CreationTimestamp)
            .ToArray();
        var latest = orders.FirstOrDefault();
        var status = latest?.Status switch
        {
            2 => "filled",
            3 => "cancelled",
            5 => "rejected",
            4 => "expired",
            _ => "unavailable"
        };
        await journal.WriteAsync(
            $"recovery-entry-resolved:order={latest?.Id}; status={status}",
            cancellationToken);
        _session.PendingEntryDirection = null;
        _session.PendingEntryOrderId = null;
        await sessionStore.SaveAsync(_session, cancellationToken);
    }

    private async Task<decimal> GetActualEntryPriceAsync(
        TradeDirection direction,
        TopstepPosition position,
        CancellationToken cancellationToken)
    {
        if (_session is null || _account is null || _contract is null)
        {
            return position.AveragePrice;
        }

        var executions = (await topstep.GetTradesAsync(
                _account.Id,
                MarketTime.ToUtc(_session.TradingDate, TimeOnly.MinValue),
                DateTimeOffset.UtcNow,
                cancellationToken))
            .Where(execution => execution.ContractId == _contract.Id &&
                                execution.Side == (direction == TradeDirection.Long ? 0 : 1) &&
                                (_session.PendingEntryOrderId is null ||
                                 execution.OrderId == _session.PendingEntryOrderId))
            .ToArray();
        var filledQuantity = executions.Sum(execution => execution.Size);
        if (filledQuantity == position.Size)
        {
            return executions.Sum(execution => execution.Price * execution.Size) / filledQuantity;
        }

        await journal.WriteAsync(
            $"recovery-entry-executions-incomplete:expected-size={position.Size}; execution-size={filledQuantity}; using-position-average={position.AveragePrice}",
            cancellationToken);
        return position.AveragePrice;
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

        var position = (await topstep.GetOpenPositionsAsync(_account.Id, cancellationToken))
            .SingleOrDefault(item => item.ContractId == _contract.Id);
        if (position is null || !MatchesTradePosition(position, trade))
        {
            await ClosePositionAsync($"position-mismatch-before-{reason}", cancellationToken);
            return false;
        }

        requestedStop = trade.Direction == TradeDirection.Long
            ? Math.Floor(requestedStop / OrbStrategy.TickSize) * OrbStrategy.TickSize
            : Math.Ceiling(requestedStop / OrbStrategy.TickSize) * OrbStrategy.TickSize;

        var existingStops = await GetContractStopOrdersAsync(cancellationToken);
        if (existingStops.Length > 1 ||
            existingStops.Length == 1 &&
            (!MatchesProtectiveStop(existingStops[0], trade) ||
             trade.StopOrderId is long savedOrderId && savedOrderId != existingStops[0].Id))
        {
            await ClosePositionAsync($"invalid-or-duplicate-stop-before-{reason}", cancellationToken);
            return false;
        }

        if (existingStops.Length == 1 && trade.StopOrderId is null)
        {
            trade.StopOrderId = existingStops[0].Id;
        }

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
        if (_account is null || _contract is null || trade.StopOrderId is null)
        {
            return false;
        }

        for (var attempt = 0; attempt < 3; attempt++)
        {
            var stops = await GetContractStopOrdersAsync(cancellationToken);
            var order = stops.SingleOrDefault();
            if (stops.Length == 1 &&
                order is not null &&
                order.Id == trade.StopOrderId &&
                MatchesProtectiveStop(order, trade) &&
                order.StopPrice == expectedStop)
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
        if (_account is null || _contract is null)
        {
            return false;
        }

        var stops = await GetContractStopOrdersAsync(cancellationToken);
        var order = stops.SingleOrDefault();
        if (stops.Length != 1 ||
            order is null ||
            !MatchesProtectiveStop(order, trade) ||
            order.StopPrice is not decimal stop)
        {
            return false;
        }

        if (trade.StopOrderId is null)
        {
            trade.StopOrderId = order.Id;
        }
        else if (order.Id != trade.StopOrderId)
        {
            return false;
        }

        trade.ConfirmedStop = stop;
        return true;
    }

    private async Task CloseForSessionEndAsync(CancellationToken cancellationToken)
    {
        if (_account is null || _contract is null)
        {
            return;
        }

        var contractIds = (await topstep.GetOpenPositionsAsync(_account.Id, cancellationToken))
            .Select(position => position.ContractId)
            .Distinct()
            .ToArray();
        foreach (var contractId in contractIds)
        {
            if (contractId == _contract.Id)
            {
                await ClosePositionAsync("4pm-et-session-close", cancellationToken);
            }
            else
            {
                await CloseAdditionalContractAsync(contractId, cancellationToken);
            }
        }
    }

    private async Task CloseOpenContractPositionAsync(string reason, CancellationToken cancellationToken)
    {
        if (_account is null || _contract is null)
        {
            return;
        }

        if (!(await topstep.GetOpenPositionsAsync(_account.Id, cancellationToken))
            .Any(position => position.ContractId == _contract.Id))
        {
            return;
        }

        await ClosePositionAsync(reason, cancellationToken);
    }

    private async Task ClosePositionAsync(string reason, CancellationToken cancellationToken)
    {
        if (_account is null || _contract is null)
        {
            return;
        }

        var trade = _session?.Trade;
        try
        {
            await topstep.ClosePositionAsync(_account.Id, _contract.Id, cancellationToken);
            await journal.WriteAsync($"position-close-submitted:{reason}", cancellationToken);
            for (var attempt = 0; attempt < 3; attempt++)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
                if (!(await topstep.GetOpenPositionsAsync(_account.Id, cancellationToken))
                    .Any(position => position.ContractId == _contract.Id))
                {
                    await ReconcileExecutionsAsync(trade, cancellationToken);
                    if (_session is not null)
                    {
                        _session.Trade = null;
                        _session.PendingEntryDirection = null;
                        await sessionStore.SaveAsync(_session, cancellationToken);
                    }

                    await journal.WriteAsync($"position-close-confirmed:{reason}", cancellationToken);
                    return;
                }
            }

            await journal.WriteAsync($"position-close-unconfirmed:{reason}", cancellationToken);
        }
        catch (Exception exception)
        {
            await journal.WriteAsync($"position-close-failed:{reason}; {exception.GetType().Name}:{exception.Message}", cancellationToken);
        }
    }

    private async Task CloseAdditionalContractAsync(string contractId, CancellationToken cancellationToken)
    {
        if (_account is null)
        {
            return;
        }

        try
        {
            await topstep.ClosePositionAsync(_account.Id, contractId, cancellationToken);
            for (var attempt = 0; attempt < 3; attempt++)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
                if (!(await topstep.GetOpenPositionsAsync(_account.Id, cancellationToken))
                    .Any(position => position.ContractId == contractId))
                {
                    await journal.WriteAsync($"position-close-confirmed:4pm-et-session-close; contract={contractId}", cancellationToken);
                    return;
                }
            }

            await journal.WriteAsync($"position-close-unconfirmed:4pm-et-session-close; contract={contractId}", cancellationToken);
        }
        catch (Exception exception)
        {
            await journal.WriteAsync($"position-close-failed:4pm-et-session-close; contract={contractId}; {exception.GetType().Name}:{exception.Message}", cancellationToken);
        }
    }

    private async Task<TopstepOrder[]> GetContractStopOrdersAsync(CancellationToken cancellationToken)
    {
        if (_account is null || _contract is null)
        {
            return [];
        }

        return (await topstep.GetOpenOrdersAsync(_account.Id, cancellationToken))
            .Where(order => order.ContractId == _contract.Id && order.Type == 4)
            .ToArray();
    }

    private static bool MatchesTradePosition(TopstepPosition position, TradeState trade) =>
        position.ContractId == trade.ContractId &&
        trade.Quantity == OrbStrategy.EntryQuantity &&
        position.Size == trade.Quantity &&
        position.Type == (trade.Direction == TradeDirection.Long ? 1 : 2);

    private static bool MatchesProtectiveStop(TopstepOrder order, TradeState trade) =>
        order.ContractId == trade.ContractId &&
        order.Status == 1 &&
        order.Type == 4 &&
        order.Side == (trade.Direction == TradeDirection.Long ? 1 : 0) &&
        order.Size == trade.Quantity;

    private async Task ReconcileExecutionsAsync(TradeState? expectedTrade, CancellationToken cancellationToken)
    {
        if (_session is null || _account is null || _contract is null)
        {
            return;
        }

        var start = MarketTime.ToUtc(_session.TradingDate, TimeOnly.MinValue);
        var executions = (await topstep.GetTradesAsync(_account.Id, start, DateTimeOffset.UtcNow, cancellationToken))
            .Where(execution => execution.ContractId == _contract.Id)
            .OrderBy(execution => execution.CreationTimestamp)
            .ToArray();
        foreach (var execution in executions.Where(execution => !_session.LoggedTradeIds.Contains(execution.Id)))
        {
            var isProtectiveStop = expectedTrade?.StopOrderId == execution.OrderId &&
                                   execution.Side == (expectedTrade.Direction == TradeDirection.Long ? 1 : 0);
            var stopSlippage = !isProtectiveStop || expectedTrade?.ConfirmedStop is not decimal expectedStop
                ? (decimal?)null
                : expectedTrade.Direction == TradeDirection.Long
                    ? expectedStop - execution.Price
                    : execution.Price - expectedStop;
            await journal.WriteAsync(
                $"actual-execution:id={execution.Id}; order={execution.OrderId}; side={execution.Side}; size={execution.Size}; price={execution.Price}; commissions={execution.Fees}; realized-pnl={execution.ProfitAndLoss}; theoretical-stop={expectedTrade?.ConfirmedStop}; stop-slippage={stopSlippage}",
                cancellationToken);
            _session.LoggedTradeIds.Add(execution.Id);
        }

        await sessionStore.SaveAsync(_session, cancellationToken);
    }
}

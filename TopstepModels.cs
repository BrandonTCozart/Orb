namespace Orb;

public sealed class TopstepOptions
{
    public const string SectionName = "Topstep";

    public string ApiBaseUrl { get; init; } = "https://api.topstepx.com";
    public string MarketHubUrl { get; init; } = "https://rtc.topstepx.com/hubs/market";
    public string AccountName { get; init; } = "50KTC-SKU-V2-DLL-384690-84340200";
    public string? UserName { get; init; } = "brandon.cozart2000@gmail.com";
    public string? ApiKey { get; init; } = "DjuaY+KGjwOEBDiUnltkJ0CdQJEQ/0vsOY70rZwEJ3c=";
    public bool Live { get; init; }
    public bool EnableStage6ReversalTrade { get; init; }
    public string DataDirectory { get; init; } = "data";
    public decimal CommissionAndSlippagePerContract { get; init; } = 10m;
}

public enum TradeDirection
{
    Long,
    Short
}

public class ApiResponse
{
    public bool Success { get; init; }
    public int ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
}

public sealed class LoginResponse : ApiResponse
{
    public string? Token { get; init; }
}

public sealed class ValidateResponse : ApiResponse
{
    public string? NewToken { get; init; }
}

public sealed class AccountSearchResponse : ApiResponse
{
    public List<TopstepAccount> Accounts { get; init; } = [];
}

public sealed class TopstepAccount
{
    public int Id { get; init; }
    public string Name { get; init; } = string.Empty;
    public bool CanTrade { get; init; }
    public bool IsVisible { get; init; }
}

public sealed class ContractResponse : ApiResponse
{
    public List<TopstepContract> Contracts { get; init; } = [];
}

public sealed class TopstepContract
{
    public string Id { get; init; } = string.Empty;
    public string Name { get; init; } = string.Empty;
    public string Description { get; init; } = string.Empty;
    public decimal TickSize { get; init; }
    public decimal TickValue { get; init; }
    public bool ActiveContract { get; init; }
    public string SymbolId { get; init; } = string.Empty;
}

public sealed class BarsResponse : ApiResponse
{
    public List<Candle> Bars { get; init; } = [];
}

public sealed class Candle
{
    public DateTimeOffset T { get; init; }
    public decimal O { get; init; }
    public decimal H { get; init; }
    public decimal L { get; init; }
    public decimal C { get; init; }
    public long V { get; init; }
}

public sealed class OrderResponse : ApiResponse
{
    public long OrderId { get; init; }
}

public sealed class OpenOrdersResponse : ApiResponse
{
    public List<TopstepOrder> Orders { get; init; } = [];
}

public sealed class OrdersResponse : ApiResponse
{
    public List<TopstepOrder> Orders { get; init; } = [];
}

public sealed class TopstepOrder
{
    public long Id { get; init; }
    public string ContractId { get; init; } = string.Empty;
    public DateTimeOffset CreationTimestamp { get; init; }
    public int Status { get; init; }
    public int Type { get; init; }
    public int Side { get; init; }
    public int Size { get; init; }
    public decimal? StopPrice { get; init; }
    public decimal? FilledPrice { get; init; }
    public int FillVolume { get; init; }
    public string? CustomTag { get; init; }
}

public sealed class PositionsResponse : ApiResponse
{
    public List<TopstepPosition> Positions { get; init; } = [];
}

public sealed class TopstepPosition
{
    public int Id { get; init; }
    public string ContractId { get; init; } = string.Empty;
    public int Type { get; init; }
    public int Size { get; init; }
    public decimal AveragePrice { get; init; }
}

public sealed class TradesResponse : ApiResponse
{
    public List<TopstepTrade> Trades { get; init; } = [];
}

public sealed class TopstepTrade
{
    public long Id { get; init; }
    public string ContractId { get; init; } = string.Empty;
    public DateTimeOffset CreationTimestamp { get; init; }
    public decimal Price { get; init; }
    public decimal? ProfitAndLoss { get; init; }
    public decimal Fees { get; init; }
    public int Side { get; init; }
    public int Size { get; init; }
    public long OrderId { get; init; }
}

public sealed class Quote
{
    public string Symbol { get; init; } = string.Empty;
    public decimal LastPrice { get; init; }
    public decimal BestBid { get; init; }
    public decimal BestAsk { get; init; }
    public DateTimeOffset Timestamp { get; init; }
}

public sealed class ReferenceLevel
{
    public string Name { get; init; } = string.Empty;
    public decimal Price { get; init; }
}

public sealed class OpeningRange
{
    public decimal High { get; init; }
    public decimal Low { get; init; }
    public decimal Midpoint => (High + Low) / 2m;
    public decimal Width => High - Low;
}

public sealed class InitialStopCalculation
{
    public decimal Stop { get; init; }
    public decimal EstimatedRisk { get; init; }
    public bool UsedFallback { get; init; }
}

public sealed class FairValueGap
{
    public bool Bullish { get; init; }
    public decimal Lower { get; init; }
    public decimal Upper { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
    public decimal Width => Upper - Lower;
}

public sealed class OrbSessionState
{
    public DateOnly TradingDate { get; init; }
    public bool TradeSubmitted { get; set; }
    public TradeDirection? PendingEntryDirection { get; set; }
    public long? PendingEntryOrderId { get; set; }
    public string? PendingEntryContractId { get; set; }
    public int? PendingEntryQuantity { get; set; }
    public bool PendingEntryIsStage6 { get; set; }
    public DateTimeOffset? LastEntryCandle { get; set; }
    public bool Stage6Eligible { get; set; }
    public bool Stage6Submitted { get; set; }
    public DateTimeOffset? Stage6EligibleAt { get; set; }
    public TradeDirection? Stage6OriginalDirection { get; set; }
    public bool Stage6RangeRevisited { get; set; }
    public DateTimeOffset? Stage6RangeRevisitedAt { get; set; }
    public TradeState? Trade { get; set; }
    public List<long> LoggedTradeIds { get; init; } = [];
}

public sealed class TradeState
{
    public TradeDirection Direction { get; init; }
    public int Quantity { get; init; }
    public string ContractId { get; init; } = string.Empty;
    public decimal EntryPrice { get; init; }
    public long? EntryOrderId { get; init; }
    public decimal PointValue { get; init; } = 50m;
    public bool IsStage6 { get; init; }
    public decimal InitialStop { get; init; }
    public long? StopOrderId { get; set; }
    public decimal? ConfirmedStop { get; set; }
    public int CompletedStage { get; set; } = 2;
    public decimal PeakProfit { get; set; }
    public decimal? Stage4PeakProfit { get; set; }
    public decimal? Stage4SecuredProfit { get; set; }
    public List<ReferenceLevel> ReferenceLevels { get; init; } = [];
    public List<decimal> UsedReferencePrices { get; init; } = [];
}

public sealed class StageAction
{
    public int Stage { get; init; }
    public string Reason { get; init; } = string.Empty;
    public decimal? DesiredStop { get; init; }
    public decimal? ReferencePrice { get; init; }
}

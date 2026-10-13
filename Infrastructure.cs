using System.Text.Json;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Options;

namespace Orb;

public static class MarketTime
{
    public static readonly TimeZoneInfo Eastern = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");

    public static DateTimeOffset ToUtc(DateOnly date, TimeOnly time)
    {
        var local = DateTime.SpecifyKind(date.ToDateTime(time), DateTimeKind.Unspecified);
        return new DateTimeOffset(TimeZoneInfo.ConvertTimeToUtc(local, Eastern), TimeSpan.Zero);
    }

    public static DateTimeOffset ToEastern(DateTimeOffset time) =>
        TimeZoneInfo.ConvertTime(time, Eastern);
}

public sealed class TradeJournal(IOptions<TopstepOptions> options, ILogger<TradeJournal> logger)
{
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public async Task WriteAsync(string message, CancellationToken cancellationToken)
    {
        var now = MarketTime.ToEastern(DateTimeOffset.UtcNow);
        var directory = Path.Combine(AppContext.BaseDirectory, options.Value.DataDirectory, "logs");
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, $"orb-{DateOnly.FromDateTime(now.Date):yyyy-MM-dd}.txt");
        var entry = $"{now:O} {message}{Environment.NewLine}";

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            await File.AppendAllTextAsync(path, entry, cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }

        logger.LogInformation("{JournalMessage}", message);
    }
}

public sealed class SessionStore(IOptions<TopstepOptions> options)
{
    private readonly JsonSerializerOptions _jsonOptions = new() { WriteIndented = true };

    public async Task<OrbSessionState> LoadAsync(DateOnly tradingDate, CancellationToken cancellationToken)
    {
        var path = GetPath(tradingDate);
        if (!File.Exists(path))
        {
            return new OrbSessionState { TradingDate = tradingDate };
        }

        await using var stream = File.OpenRead(path);
        return await JsonSerializer.DeserializeAsync<OrbSessionState>(stream, _jsonOptions, cancellationToken)
               ?? new OrbSessionState { TradingDate = tradingDate };
    }

    public async Task SaveAsync(OrbSessionState state, CancellationToken cancellationToken)
    {
        var path = GetPath(state.TradingDate);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var temporaryPath = $"{path}.tmp";
        await using (var stream = File.Create(temporaryPath))
        {
            await JsonSerializer.SerializeAsync(stream, state, _jsonOptions, cancellationToken);
        }

        File.Move(temporaryPath, path, true);
    }

    private string GetPath(DateOnly tradingDate) =>
        Path.Combine(AppContext.BaseDirectory, options.Value.DataDirectory, "state", $"orb-{tradingDate:yyyy-MM-dd}.json");
}

public sealed class MarketDataFeed(
    TopstepClient client,
    IOptions<TopstepOptions> options,
    ILogger<MarketDataFeed> logger)
    : IAsyncDisposable
{
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private HubConnection? _connection;
    private string? _contractId;
    private string? _token;

    public Quote? LastQuote { get; private set; }
    public event Func<Quote, Task>? QuoteReceived;

    public async Task EnsureStartedAsync(string contractId, CancellationToken cancellationToken)
    {
        var token = await client.GetSessionTokenAsync(cancellationToken);
        await _connectionLock.WaitAsync(cancellationToken);
        try
        {
            if (_connection is { State: HubConnectionState.Connected } &&
                _contractId == contractId &&
                _token == token)
            {
                return;
            }

            if (_connection is not null)
            {
                await _connection.DisposeAsync();
            }

            _contractId = contractId;
            _token = token;
            _connection = new HubConnectionBuilder()
                .WithUrl(options.Value.MarketHubUrl, hubOptions =>
                {
                    hubOptions.AccessTokenProvider = () => Task.FromResult<string?>(token);
                    hubOptions.SkipNegotiation = true;
                    hubOptions.Transports = HttpTransportType.WebSockets;
                })
                .WithAutomaticReconnect()
                .Build();

            _connection.On<string, Quote>("GatewayQuote", async (eventContractId, quote) =>
            {
                if (eventContractId == _contractId && quote.LastPrice > 0)
                {
                    LastQuote = quote;
                    var handler = QuoteReceived;
                    if (handler is not null)
                    {
                        await handler(quote);
                    }
                }
            });
            _connection.Reconnected += async _ =>
            {
                if (_connection is not null && _contractId is not null)
                {
                    await _connection.InvokeAsync("SubscribeContractQuotes", _contractId);
                }
            };
            _connection.Closed += error =>
            {
                logger.LogWarning(error, "Topstep market-data connection closed.");
                return Task.CompletedTask;
            };

            await _connection.StartAsync(cancellationToken);
            await _connection.InvokeAsync("SubscribeContractQuotes", contractId, cancellationToken);
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
        }

        _connectionLock.Dispose();
    }
}

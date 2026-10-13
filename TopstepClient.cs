using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using Microsoft.Extensions.Options;

namespace Orb;

public sealed class TopstepClient(HttpClient httpClient, IOptions<TopstepOptions> options)
{
    private readonly SemaphoreSlim _authenticationLock = new(1, 1);
    private string? _sessionToken;
    private DateTimeOffset _sessionExpiry;

    public async Task<string> GetSessionTokenAsync(CancellationToken cancellationToken, bool forceLogin = false)
    {
        await _authenticationLock.WaitAsync(cancellationToken);
        try
        {
            if (!forceLogin && !string.IsNullOrEmpty(_sessionToken) && _sessionExpiry > DateTimeOffset.UtcNow.AddMinutes(10))
            {
                return _sessionToken;
            }

            if (!forceLogin && !string.IsNullOrEmpty(_sessionToken))
            {
                try
                {
                    var validated = await PostUnauthenticatedAsync<ValidateResponse>(
                        "/api/Auth/validate", new { }, _sessionToken, cancellationToken);
                    if (validated.Success && !string.IsNullOrWhiteSpace(validated.NewToken))
                    {
                        _sessionToken = validated.NewToken;
                        _sessionExpiry = DateTimeOffset.UtcNow.AddHours(23);
                        return _sessionToken;
                    }
                }
                catch (UnauthorizedAccessException)
                {
                    _sessionToken = null;
                    _sessionExpiry = default;
                }
            }

            if (string.IsNullOrWhiteSpace(options.Value.UserName) || string.IsNullOrWhiteSpace(options.Value.ApiKey))
            {
                throw new InvalidOperationException(
                    "Topstep:UserName and Topstep:ApiKey must be supplied through environment variables or user secrets.");
            }

            var login = await PostUnauthenticatedAsync<LoginResponse>(
                "/api/Auth/loginKey",
                new { userName = options.Value.UserName, apiKey = options.Value.ApiKey },
                null,
                cancellationToken);

            if (!login.Success || string.IsNullOrWhiteSpace(login.Token))
            {
                throw new InvalidOperationException($"Topstep authentication failed: {login.ErrorCode} {login.ErrorMessage}");
            }

            _sessionToken = login.Token;
            _sessionExpiry = DateTimeOffset.UtcNow.AddHours(23);
            return _sessionToken;
        }
        finally
        {
            _authenticationLock.Release();
        }
    }

    public async Task<TopstepAccount> GetTradingAccountAsync(CancellationToken cancellationToken)
    {
        var result = await PostAsync<AccountSearchResponse>(
            "/api/Account/search", new { onlyActiveAccounts = true }, cancellationToken);
        var account = result.Accounts.SingleOrDefault(account =>
            string.Equals(account.Name, options.Value.AccountName, StringComparison.OrdinalIgnoreCase));

        return account is { CanTrade: true, IsVisible: true }
            ? account
            : throw new InvalidOperationException($"Configured account '{options.Value.AccountName}' is unavailable or cannot trade.");
    }

    public async Task<TopstepContract> GetActiveEsContractAsync(CancellationToken cancellationToken)
    {
        var result = await PostAsync<ContractResponse>(
            "/api/Contract/available", new { live = options.Value.Live }, cancellationToken);

        return result.Contracts.SingleOrDefault(contract =>
                   contract.ActiveContract &&
                   contract.SymbolId == "F.US.EP" &&
                   contract.TickSize == 0.25m &&
                   contract.TickValue == 12.5m)
               ?? throw new InvalidOperationException("No active E-mini S&P 500 contract is available.");
    }

    public async Task<IReadOnlyList<Candle>> GetMinuteBarsAsync(
        string contractId,
        DateTimeOffset start,
        DateTimeOffset end,
        CancellationToken cancellationToken)
    {
        var result = await PostAsync<BarsResponse>(
            "/api/History/retrieveBars",
            new
            {
                contractId,
                live = options.Value.Live,
                startTime = start,
                endTime = end,
                unit = 2,
                unitNumber = 1,
                limit = 20_000,
                includePartialBar = false
            },
            cancellationToken);
        return result.Bars.OrderBy(bar => bar.T).ToArray();
    }

    public Task<OrderResponse> PlaceOrderAsync(
        int accountId,
        string contractId,
        int type,
        int side,
        int size,
        decimal? stopPrice,
        string? customTag,
        CancellationToken cancellationToken) =>
        PostAsync<OrderResponse>(
            "/api/Order/place",
            new
            {
                accountId,
                contractId,
                type,
                side,
                size,
                limitPrice = (decimal?)null,
                stopPrice,
                trailPrice = (decimal?)null,
                customTag,
                stopLossBracket = (object?)null,
                takeProfitBracket = (object?)null
            },
            cancellationToken);

    public Task<ApiResponse> ModifyOrderAsync(
        int accountId,
        long orderId,
        int size,
        decimal stopPrice,
        CancellationToken cancellationToken) =>
        PostAsync<ApiResponse>(
            "/api/Order/modify",
            new { accountId, orderId, size, limitPrice = (decimal?)null, stopPrice, trailPrice = (decimal?)null },
            cancellationToken);

    public async Task<IReadOnlyList<TopstepOrder>> GetOpenOrdersAsync(int accountId, CancellationToken cancellationToken)
    {
        var result = await PostAsync<OpenOrdersResponse>("/api/Order/searchOpen", new { accountId }, cancellationToken);
        return result.Orders;
    }

    public async Task<IReadOnlyList<TopstepPosition>> GetOpenPositionsAsync(int accountId, CancellationToken cancellationToken)
    {
        var result = await PostAsync<PositionsResponse>("/api/Position/searchOpen", new { accountId }, cancellationToken);
        return result.Positions;
    }

    public Task<ApiResponse> ClosePositionAsync(int accountId, string contractId, CancellationToken cancellationToken) =>
        PostAsync<ApiResponse>("/api/Position/closeContract", new { accountId, contractId }, cancellationToken);

    public async Task<IReadOnlyList<TopstepTrade>> GetTradesAsync(
        int accountId,
        DateTimeOffset start,
        DateTimeOffset end,
        CancellationToken cancellationToken)
    {
        var result = await PostAsync<TradesResponse>(
            "/api/Trade/search",
            new { accountId, startTimestamp = start, endTimestamp = end },
            cancellationToken);
        return result.Trades;
    }

    private async Task<T> PostAsync<T>(string path, object payload, CancellationToken cancellationToken)
        where T : ApiResponse
    {
        var token = await GetSessionTokenAsync(cancellationToken);
        T response;
        try
        {
            response = await PostUnauthenticatedAsync<T>(path, payload, token, cancellationToken);
        }
        catch (UnauthorizedAccessException)
        {
            var rejectedCurrentToken = await InvalidateSessionTokenAsync(token, cancellationToken);
            token = await GetSessionTokenAsync(cancellationToken, rejectedCurrentToken);
            response = await PostUnauthenticatedAsync<T>(path, payload, token, cancellationToken);
        }

        if (response.Success)
        {
            return response;
        }

        throw new InvalidOperationException($"Topstep {path} failed: {response.ErrorCode} {response.ErrorMessage}");
    }

    private async Task<T> PostUnauthenticatedAsync<T>(
        string path,
        object payload,
        string? token,
        CancellationToken cancellationToken)
        where T : ApiResponse
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(new Uri(options.Value.ApiBaseUrl), path))
        {
            Content = JsonContent.Create(payload)
        };
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/plain"));
        if (!string.IsNullOrEmpty(token))
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        }

        using var response = await httpClient.SendAsync(request, cancellationToken);
        if (response.StatusCode == HttpStatusCode.Unauthorized)
        {
            throw new UnauthorizedAccessException("Topstep session token expired or was rejected.");
        }

        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<T>(cancellationToken: cancellationToken)
               ?? throw new InvalidOperationException($"Topstep {path} returned an empty response.");
    }

    private async Task<bool> InvalidateSessionTokenAsync(string rejectedToken, CancellationToken cancellationToken)
    {
        await _authenticationLock.WaitAsync(cancellationToken);
        try
        {
            if (!string.Equals(_sessionToken, rejectedToken, StringComparison.Ordinal))
            {
                return false;
            }

            _sessionToken = null;
            _sessionExpiry = default;
            return true;
        }
        finally
        {
            _authenticationLock.Release();
        }
    }
}

using System.Globalization;
using Microsoft.Extensions.Options;

namespace Orb;

/// <summary>
/// Preloaded, editable schedule of major economic release dates.
/// Reads the persistent data-folder override if present, otherwise the
/// event schedule shipped alongside Orb.dll. No network dependency.
/// </summary>
public sealed class EconomicReleaseCalendar(IOptions<TopstepOptions> options)
{
    private Dictionary<DateOnly, string[]> _events = new();
    private DateOnly _coverageThrough = DateOnly.MinValue;
    private string? _loadError;

    public void Reload()
    {
        _events = new();
        _coverageThrough = DateOnly.MinValue;
        _loadError = null;

        var overridePath = Path.Combine(AppContext.BaseDirectory,
            options.Value.DataDirectory, "economic_events.csv");
        var bundledPath = Path.Combine(AppContext.BaseDirectory, "economic_events.csv");
        var path = File.Exists(overridePath) ? overridePath : bundledPath;

        try
        {
            if (!File.Exists(path))
                throw new FileNotFoundException("No economic_events.csv was found.", path);

            var events = new Dictionary<DateOnly, List<string>>();
            var hasHeader = false;
            foreach (var raw in File.ReadLines(path))
            {
                var line = raw.Trim().TrimStart('\uFEFF');
                if (string.IsNullOrEmpty(line)) continue;
                if (line.StartsWith("# coverage_through_et=", StringComparison.OrdinalIgnoreCase))
                {
                    _coverageThrough = ParseDay(line["# coverage_through_et=".Length..]);
                    continue;
                }
                if (line.StartsWith('#')) continue;
                if (line.Equals("date_et,event_code,source_url", StringComparison.OrdinalIgnoreCase))
                {
                    hasHeader = true;
                    continue;
                }

                var values = line.Split(',', 3);
                if (!hasHeader || values.Length != 3 ||
                    string.IsNullOrWhiteSpace(values[1]))
                    throw new FormatException("Invalid economic calendar row: " + line);

                var day = ParseDay(values[0]);
                if (!events.TryGetValue(day, out var codes))
                    events[day] = codes = new List<string>();
                codes.Add(values[1].Trim());
            }

            if (!hasHeader || _coverageThrough == DateOnly.MinValue || events.Count == 0)
                throw new FormatException("Economic calendar is missing dates, header, or coverage_through_et.");

            _events = events.ToDictionary(
                x => x.Key, x => x.Value.Distinct(StringComparer.OrdinalIgnoreCase).ToArray());
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or FormatException or ArgumentException)
        {
            _events = new();
            _coverageThrough = DateOnly.MinValue;
            _loadError = error.Message;
        }
    }

    /// <summary>Returns null if the date is safe for a NEW trade, otherwise why it is blocked.</summary>
    public string? NoEntryReason(DateOnly day)
    {
        if (_loadError is not null)
            return "economic-calendar-unavailable:" + _loadError;
        if (day > _coverageThrough)
            return "economic-calendar-expired:coverage-through=" +
                   _coverageThrough.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        return _events.TryGetValue(day, out var codes)
            ? "scheduled-major-release-day:" + string.Join("+", codes)
            : null;
    }

    public string CoverageNotice(DateOnly day)
    {
        if (_loadError is not null)
            return "economic-calendar-update-required:" + _loadError +
                   "; new entries disabled until the calendar is corrected";
        if (day > _coverageThrough)
            return "economic-calendar-EXPIRED:coverage-through=" +
                   _coverageThrough.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) +
                   "; load new verified event dates and update coverage_through_et; new entries disabled";

        var remaining = _coverageThrough.DayNumber - day.DayNumber;
        if (remaining <= 30)
            return "economic-calendar-update-soon:coverage-through=" +
                   _coverageThrough.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) +
                   "; " + remaining + " calendar days remain; add confirmed JOLTS and ISM dates";

        return "economic-calendar-active:coverage-through=" +
               _coverageThrough.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
    }

    private static DateOnly ParseDay(string input) =>
        DateOnly.ParseExact(input.Trim(), "yyyy-MM-dd", CultureInfo.InvariantCulture);
}

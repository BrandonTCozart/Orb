using Orb;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.Configure<TopstepOptions>(builder.Configuration.GetSection(TopstepOptions.SectionName));
builder.Services.AddSingleton(new HttpClient { Timeout = TimeSpan.FromSeconds(15) });
builder.Services.AddSingleton<TopstepClient>();
builder.Services.AddSingleton<TradeJournal>();
builder.Services.AddSingleton<SessionStore>();
builder.Services.AddSingleton<MarketDataFeed>();
builder.Services.AddSingleton<OrbStrategy>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();

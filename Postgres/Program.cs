var builder = WebApplication.CreateBuilder(args);

// newton to parse config object as body param
builder.Services.AddControllers().AddNewtonsoftJson();
builder.Services.AddHostedService<EventBackgroundService>();
builder.Services.AddHealthChecks();

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddOptions();

builder.Services.AddHttpClient("default").ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler
{
    UseProxy = false,
    Proxy = null,
    UseCookies = false,
    AllowAutoRedirect = false,
    PreAuthenticate = false,
});

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}


app.MapControllers();

app.MapHealthChecks("/health");

app.Run();

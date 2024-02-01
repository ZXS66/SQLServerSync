using ClosedXML.Excel;
using CsvHelper;
using Quartz.Impl;
using Quartz;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using SQLServerSync;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;

#region legacy version, run program via simple cli argument
//if (args.Length > 0 && (args.ElementAt(0) == "-forever" || args.ElementAt(0) == "--forever"))
//{
//    runForever();
//}
//else
//{
//    runOnce();
//}

static void runForever()
{
    do
    {
        runOnce();
        //Console.WriteLine("Congratulations!");
        Console.WriteLine("Congratulations!!!");
        int interval = int.Parse(ConfigurationManager.AppSettings["intervalInSecond"] ?? "86400");
        Console.WriteLine("next processing time: " + DateTime.Now.AddSeconds(interval));
        Thread.Sleep(interval * 1000);
    } while (true);
}

static void runOnce()
{
    try
    {
        var syncConfig = new SQLServerSync.SyncConfig();
        new SQLServerSync.SyncProcessor(syncConfig).Process();
    }
    catch (Exception e)
    {
        Console.WriteLine(e?.InnerException?.Message ?? e?.Message);
        Console.WriteLine("press any key to continue...");
    }
}

#endregion

#region branding new version, support cron expression to schedule job by adding Quartz package

using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddConsole());
ILogger logger = factory.CreateLogger("Program");
logger.LogWarning("Program is starting...");

var builder = Host.CreateDefaultBuilder().ConfigureServices((ctx, services) =>
{
    services.AddQuartz(q =>
    {
        q.UseMicrosoftDependencyInjectionJobFactory();
    });
    services.AddQuartzHostedService(opt =>
    {
        opt.WaitForJobsToComplete = true;
    });
}).Build();
logger.LogWarning("add Quartz success!");

var schedulerFactory = builder.Services.GetRequiredService<ISchedulerFactory>();
var scheduler = await schedulerFactory.GetScheduler();

// define the job and tie it to our HelloJob class
var job = JobBuilder.Create<SyncJob>().Build();

// Trigger the job
var cronSchedule = ConfigurationManager.AppSettings.Get("cronSchedule");

var trigger = String.IsNullOrEmpty(cronSchedule)
    ? TriggerBuilder.Create().StartNow().Build()
    : TriggerBuilder.Create().WithCronSchedule(cronSchedule).Build();

await scheduler.ScheduleJob(job, trigger);

// will block until the last running job completes
await builder.RunAsync();

logger.LogInformation("program is existing");
#endregion

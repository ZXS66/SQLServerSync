using Quartz;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLServerSync;
public class SyncJob : IJob
{
    // have a public key that is easy reference in DI configuration for example
    // group helps you with targeting specific jobs in maintenance operations, 
    // like pause all jobs in group "integration"
    public static readonly JobKey Key = new JobKey(nameof(SyncJob), nameof(SQLServerSync));

    public async Task Execute(IJobExecutionContext context)
    {
        try
        {
            // get data out of the MergedJobDataMap
            //var value = context.MergedJobDataMap.GetString("some-value");
            SyncConfig config = new SyncConfig();
            SyncProcessor processor = new SyncProcessor(config);
            processor.Process();
            // do some clean work
            await Task.CompletedTask;
        }
        catch (Exception ex)
        {
            // do you want the job to refire?
            throw new JobExecutionException(msg: ex.InnerException?.Message ?? ex.Message, refireImmediately: true, cause: ex);
        }
    }
}

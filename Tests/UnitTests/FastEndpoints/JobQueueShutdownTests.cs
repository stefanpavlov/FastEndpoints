using System.Collections.Concurrent;
using FastEndpoints;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace JobQueue;

// Command whose handler catches cancellation and returns normally,
// allowing ExecuteCommand to reach the MarkJobAsCompleteAsync path.
public class ShutdownTestCommand : ICommand
{
    public static TaskCompletionSource StartedTcs = new();
    public int DelayMs { get; set; }
}

public class ShutdownTestCommandHandler : ICommandHandler<ShutdownTestCommand>
{
    public async Task ExecuteAsync(ShutdownTestCommand cmd, CancellationToken ct)
    {
        ShutdownTestCommand.StartedTcs.TrySetResult();

        try
        {
            await Task.Delay(cmd.DelayMs, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Graceful shutdown: return normally so ExecuteCommand
            // proceeds to MarkJobAsCompleteAsync instead of the failure path.
        }
    }
}

public class ShutdownTestRecord : IJobStorageRecord
{
    public string QueueID { get; set; } = "";
    public Guid TrackingID { get; set; }
    public object Command { get; set; } = null!;
    public DateTime ExecuteAfter { get; set; }
    public DateTime ExpireOn { get; set; }
    public bool IsComplete { get; set; }
}

public class ShutdownTestStorage : IJobStorageProvider<ShutdownTestRecord>
{
    internal readonly ConcurrentDictionary<Guid, ShutdownTestRecord> Store = new();

    public bool DistributedJobProcessingEnabled => false;

    public Task StoreJobAsync(ShutdownTestRecord r, CancellationToken ct)
    {
        Store[r.TrackingID] = r;

        return Task.CompletedTask;
    }

    public Task<ICollection<ShutdownTestRecord>> GetNextBatchAsync(PendingJobSearchParams<ShutdownTestRecord> p)
    {
        var match = p.Match.Compile();
        var batch = Store.Values.Where(match).Take(p.Limit).ToList();

        return Task.FromResult<ICollection<ShutdownTestRecord>>(batch);
    }

    public Task MarkJobAsCompleteAsync(ShutdownTestRecord r, CancellationToken ct)
    {
        if (Store.TryGetValue(r.TrackingID, out var j))
            j.IsComplete = true;

        return Task.CompletedTask;
    }

    public Task CancelJobAsync(Guid trackingId, CancellationToken ct) => Task.CompletedTask;
    public Task OnHandlerExecutionFailureAsync(ShutdownTestRecord r, Exception ex, CancellationToken ct) => Task.CompletedTask;
    public Task PurgeStaleJobsAsync(StaleJobSearchParams<ShutdownTestRecord> p) => Task.CompletedTask;
}

// This test creates its own host and sets ServiceResolver.Instance via UseMessaging().
// It must run in isolation because other test classes (e.g. EndpointDataTests, WebTests)
// also mutate ServiceResolver.Instance — a global static — and xUnit runs classes in parallel.
// Run with: dotnet test --filter "FullyQualifiedName~Shutdown"
public class JobQueueShutdownTests
{
    [Fact]
    public async Task shutdown_drains_inflight_jobs_before_exiting()
    {
        // Build a minimal host with job queue support.
        var builder = Host.CreateApplicationBuilder([]);
        builder.Logging.ClearProviders();
        builder.Services.AddJobQueues<ShutdownTestRecord, ShutdownTestStorage>();
        var host = builder.Build();

        // Initialize messaging (sets up ServiceResolver for handler resolution)
        // but don't call UseJobQueues which would create queues for ALL loaded command types.
        host.Services.UseMessaging();

        // Start the host so IHostApplicationLifetime is active.
        await host.StartAsync();

        try
        {
            // Resolve and start just our queue.
            var queue = host.Services.GetRequiredService<
                JobQueue<ShutdownTestCommand, FastEndpoints.Void, ShutdownTestRecord, ShutdownTestStorage>>();

            ShutdownTestCommand.StartedTcs = new();
            queue.SetLimits(2, TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(100));

            // Queue a long-running job.
            var cmd = new ShutdownTestCommand { DelayMs = 30_000 };
            await cmd.QueueJobAsync();

            // Wait for the handler to start executing.
            await ShutdownTestCommand.StartedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

            // Trigger shutdown while the job is running.
            // The linked CTS cancels the handler, which catches it and returns normally.
            // ExecuteCommand then calls MarkJobAsCompleteAsync.
            // The drain should wait for RunJob to finish before CommandExecutorTask returns.
            await host.StopAsync(TimeSpan.FromSeconds(10));

            // Give the drain a moment to complete (it runs after ApplicationStopping fires,
            // outside the host's hosted-service lifecycle).
            var deadline = DateTime.UtcNow.AddSeconds(5);

            var storage = host.Services.GetRequiredService<ShutdownTestStorage>();

            while (DateTime.UtcNow < deadline)
            {
                if (storage.Store.Values.Any(r => r.IsComplete))
                    break;

                await Task.Delay(50);
            }

            // The job record should be marked complete — the drain waited for
            // MarkJobAsCompleteAsync to run before the executor exited.
            storage.Store.Values.ShouldContain(r => r.IsComplete);
        }
        finally
        {
            host.Dispose();
        }
    }
}

using System.Collections.Concurrent;
using System.Diagnostics;

namespace TestCases.JobQueueTest;

public class JobConcurrencyTestCommand : ICommand
{
    public static readonly ConcurrentDictionary<int, (long StartTicks, long EndTicks)> ExecutionLog = new();
    public static int CompletedCount;

    public int Id { get; set; }
    public int DelayMs { get; set; }
}

sealed class JobConcurrencyTestCommandHandler : ICommandHandler<JobConcurrencyTestCommand>
{
    public async Task ExecuteAsync(JobConcurrencyTestCommand cmd, CancellationToken ct)
    {
        var start = Stopwatch.GetTimestamp();
        await Task.Delay(cmd.DelayMs, ct);
        var end = Stopwatch.GetTimestamp();
        JobConcurrencyTestCommand.ExecutionLog[cmd.Id] = (start, end);
        Interlocked.Increment(ref JobConcurrencyTestCommand.CompletedCount);
    }
}

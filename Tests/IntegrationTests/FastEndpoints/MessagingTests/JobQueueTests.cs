using System.Diagnostics;
using TestCases.EventHandlingTest;
using TestCases.JobQueueTest;

namespace Messaging;

public class JobQueueTests(Sut App) : TestBase<Sut>
{
    public static readonly TheoryData<DateTime?, DateTime?> JobCreateCases = new()
    {
        { null, null },
        { null, DateTime.UtcNow },
        { DateTime.UtcNow, null },
        { DateTime.UtcNow, DateTime.UtcNow }
    };

    [Theory, MemberData(nameof(JobCreateCases)), Priority(1)]
    public async Task Jobs_Create(DateTime? executeAfter, DateTime? expireOn)
    {
        var cmd = new JobTestCommand();
        var job = cmd.CreateJob<Job>(executeAfter, expireOn);

        job.CommandType.ShouldBe(typeof(JobTestCommand).FullName);

        if (executeAfter.HasValue)
            Assert.Equal(job.ExecuteAfter, executeAfter);
        else
            Assert.Equal(DateTime.UtcNow, job.ExecuteAfter, TimeSpan.FromMilliseconds(100));

        if (expireOn.HasValue)
            Assert.Equal(job.ExpireOn, expireOn);
        else
            Assert.Equal(DateTime.UtcNow.AddHours(4), job.ExpireOn, TimeSpan.FromMilliseconds(100));
    }

    public static readonly TheoryData<DateTime?, DateTime?> JobCreateExceptionCases = new()
    {
        { DateTime.Now, DateTime.Now },
        { DateTime.Now, null },
        { null, DateTime.Now }
    };

    [Theory, MemberData(nameof(JobCreateExceptionCases)), Priority(2)]
    public async Task Jobs_Create_Exception(DateTime? executeAfter, DateTime? expireOn)
    {
        var cmd = new JobTestCommand();
        Assert.Throws<ArgumentException>(() => cmd.CreateJob<Job>(executeAfter, expireOn));
    }

    [Fact, Priority(3), Trait("ExcludeInCiCd", "Yes")]
    public async Task Job_Cancellation()
    {
        var cts = new CancellationTokenSource(5000);
        var jobs = new List<JobCancelTestCommand>();

        await Parallel.ForAsync(
            0,
            10,
            cts.Token,
            async (_, ct) =>
            {
                var job = new JobCancelTestCommand();
                jobs.Add(job);
                job.TrackingId = await job.QueueJobAsync(ct: ct);
            });

        while (!cts.IsCancellationRequested && !jobs.TrueForAll(j => j.Counter > 0))
            await Task.Delay(250, cts.Token);

        var jobTracker = App.Services.GetRequiredService<IJobTracker<JobCancelTestCommand>>();

        foreach (var j in jobs)
            _ = jobTracker.CancelJobAsync(j.TrackingId, cts.Token);

        while (!cts.IsCancellationRequested && !jobs.TrueForAll(j => j.IsCancelled))
            await Task.Delay(100, cts.Token);

        jobs.ShouldContain(j => j.IsCancelled && j.Counter > 0);
        JobStorage.Jobs.Clear();
    }

    [Fact, Priority(4)]
    public async Task Jobs_Execution()
    {
        var cts = new CancellationTokenSource(5000);

        for (var i = 0; i < 10; i++)
        {
            var cmd = new JobTestCommand
            {
                Id = i,
                ShouldThrow = i == 0
            };
            await cmd.QueueJobAsync(executeAfter: i == 1 ? DateTime.UtcNow.AddDays(1) : DateTime.UtcNow, ct: cts.Token);
        }

        while (!cts.IsCancellationRequested && JobTestCommand.CompletedIDs.Count < 9)
            await Task.Delay(100, Cancellation);

        JobTestCommand.CompletedIDs.Count.ShouldBe(9);
        var expected = new[] { 0, 2, 3, 4, 5, 6, 7, 8, 9 };
        JobTestCommand.CompletedIDs.Except(expected).Any().ShouldBeFalse();
        JobStorage.Jobs.Clear();
    }

    [Fact, Priority(5)]
    public async Task Job_Deferred_Execution()
    {
        var cts = new CancellationTokenSource(5000);

        for (var i = 0; i < 10; i++)
        {
            var cmd = new JobTestCommand
            {
                Id = i,
                ShouldThrow = i == 0
            };
            var job = cmd.CreateJob<Job>(executeAfter: i == 1 ? DateTime.UtcNow.AddDays(1) : DateTime.UtcNow);
            JobStorage.Jobs.Add(job);
        }

        JobQueueExtensions.TriggerJobExecution<JobTestCommand>();

        while (!cts.IsCancellationRequested && JobTestCommand.CompletedIDs.Count < 9)
            await Task.Delay(100, Cancellation);

        JobTestCommand.CompletedIDs.Count.ShouldBe(9);
        var expected = new[] { 0, 2, 3, 4, 5, 6, 7, 8, 9 };
        JobTestCommand.CompletedIDs.Except(expected).Any().ShouldBeFalse();
        JobStorage.Jobs.Clear();
    }

    [Fact, Priority(6)]
    public async Task Job_With_Result_Execution()
    {
        var cts = new CancellationTokenSource(5000);
        var guid = Guid.NewGuid();
        var job = new JobWithResultTestCommand { Id = guid };
        var trackingId = await job.QueueJobAsync(ct: cts.Token);
        var jobTracker = App.Services.GetRequiredService<IJobTracker<JobWithResultTestCommand>>();

        while (!cts.IsCancellationRequested)
        {
            var result = await jobTracker.GetJobResultAsync<Guid>(trackingId, cts.Token);

            if (result == default)
            {
                await Task.Delay(100, cts.Token);

                continue;
            }

            result.ShouldBe(guid);

            break;
        }
    }

    [Fact, Priority(7)]
    public async Task Job_Progress_Tracking()
    {
        var name = Guid.NewGuid().ToString();
        var job = new JobProgressTestCommand { Name = name };
        var trackingId = await job.QueueJobAsync(ct: Cancellation);
        var step = 0;
        JobResult<string>? res;

        while (true)
        {
            res = await JobTracker<JobProgressTestCommand>.GetJobResultAsync<JobResult<string>>(trackingId, Cancellation);
            step = res?.CurrentStep ?? 0;

            if (step > 0)
                JobProgressTestCommand.CurrentStep = step;

            if (step == 3)
            {
                await Task.Delay(200);

                break;
            }

            await Task.Delay(100);
        }

        res!.Result.ShouldBe(name);
    }

    [Fact, Priority(8)]
    public async Task Job_Generic_Command()
    {
        var cts = new CancellationTokenSource(5000);

        var cmd = new JobTestGenericCommand<SomeEvent>
        {
            Id = 1,
            Event = new()
        };
        await cmd.QueueJobAsync(ct: cts.Token);

        while (!cts.IsCancellationRequested && JobTestGenericCommand<SomeEvent>.CompletedIDs.Count == 0)
            await Task.Delay(100, Cancellation);

        JobTestGenericCommand<SomeEvent>.CompletedIDs.Count.ShouldBe(1);
        JobTestGenericCommand<SomeEvent>.CompletedIDs.Contains(1);
        JobStorage.Jobs.Clear();
    }

    [Fact, Priority(9)]
    public async Task Job_Parallel_Execution()
    {
        // Verifies that a second job starts while the first is still running,
        // without waiting for the storage probe interval.
        //
        // Setup: MaxConcurrency = 2, StorageProbeDelay = 100ms.
        // Job 1: slow (2000ms), Job 2: fast (100ms), both queued together.
        // Job 2 must start before Job 1 finishes and within 500ms of queuing
        // (well under the 2000ms Job 1 duration).

        var cts = new CancellationTokenSource(10000);
        JobConcurrencyTestCommand.ExecutionLog.Clear();
        Interlocked.Exchange(ref JobConcurrencyTestCommand.CompletedCount, 0);

        var queuedAt = Stopwatch.GetTimestamp();
        await new JobConcurrencyTestCommand { Id = 1, DelayMs = 2000 }.QueueJobAsync(ct: cts.Token);
        await new JobConcurrencyTestCommand { Id = 2, DelayMs = 100 }.QueueJobAsync(ct: cts.Token);

        while (!cts.IsCancellationRequested && Volatile.Read(ref JobConcurrencyTestCommand.CompletedCount) < 2)
            await Task.Delay(100, cts.Token);

        Volatile.Read(ref JobConcurrencyTestCommand.CompletedCount).ShouldBe(2);

        var log = JobConcurrencyTestCommand.ExecutionLog;

        // Job 2 started while Job 1 was still running (parallel, not serial)
        log[2].StartTicks.ShouldBeLessThan(log[1].EndTicks);

        // Job 2 started promptly — within 500ms of queuing (semaphore wake, not probe delay)
        var job2Latency = Stopwatch.GetElapsedTime(queuedAt, log[2].StartTicks);
        job2Latency.ShouldBeLessThan(TimeSpan.FromMilliseconds(500));

        JobStorage.Jobs.Clear();
    }

    [Fact, Priority(10)]
    public async Task Job_Sliding_Window_Concurrency()
    {
        // Verifies that when a job completes, its slot is immediately available for a new job
        // without waiting for other in-flight jobs to finish (sliding window vs batch-and-wait).
        //
        // Setup: MaxConcurrency = 2 for this command type.
        // Job 1: slow (2000ms), Job 2: fast (100ms), Job 3: queued after Job 2 finishes.
        // With sliding window: Job 3 starts while Job 1 is still running.
        // With batch-and-wait: Job 3 can't start until Job 1 finishes.

        var cts = new CancellationTokenSource(10000);
        JobConcurrencyTestCommand.ExecutionLog.Clear();
        Interlocked.Exchange(ref JobConcurrencyTestCommand.CompletedCount, 0);

        await new JobConcurrencyTestCommand { Id = 1, DelayMs = 2000 }.QueueJobAsync(ct: cts.Token);
        await new JobConcurrencyTestCommand { Id = 2, DelayMs = 100 }.QueueJobAsync(ct: cts.Token);

        // wait for Job 2 to finish and its slot to free up
        await Task.Delay(500, cts.Token);

        await new JobConcurrencyTestCommand { Id = 3, DelayMs = 100 }.QueueJobAsync(ct: cts.Token);

        while (!cts.IsCancellationRequested && Volatile.Read(ref JobConcurrencyTestCommand.CompletedCount) < 3)
            await Task.Delay(100, cts.Token);

        Volatile.Read(ref JobConcurrencyTestCommand.CompletedCount).ShouldBe(3);

        var log = JobConcurrencyTestCommand.ExecutionLog;
        log.ContainsKey(1).ShouldBeTrue();
        log.ContainsKey(2).ShouldBeTrue();
        log.ContainsKey(3).ShouldBeTrue();

        // Job 3 filled the slot freed by Job 2 (causal chain: Job 2 ends → slot frees → Job 3 starts)
        var job2End = log[2].EndTicks;
        var job3Start = log[3].StartTicks;
        job3Start.ShouldBeGreaterThan(job2End);

        // Job 3 started before Job 1 ended (proving sliding window, not batch-and-wait)
        var job1End = log[1].EndTicks;
        job3Start.ShouldBeLessThan(job1End);

        JobStorage.Jobs.Clear();
    }
}
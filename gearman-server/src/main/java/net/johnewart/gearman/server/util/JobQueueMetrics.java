package net.johnewart.gearman.server.util;

import net.johnewart.gearman.engine.queue.JobQueue;
import net.johnewart.shuzai.DifferentialTimeSeries;
import net.johnewart.shuzai.Frequency;
import net.johnewart.shuzai.SampleMethod;
import net.johnewart.shuzai.TimeSeries;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JobQueueMetrics {
    public final TimeSeries queued, failed, completed, exceptions,
                            highJobs, midJobs, lowJobs, futureJobs;

    public JobQueueMetrics() {
        this.highJobs = new TimeSeries();
        this.midJobs = new TimeSeries();
        this.lowJobs = new TimeSeries();
        this.futureJobs = new TimeSeries();
        this.queued = new DifferentialTimeSeries();
        this.failed = new DifferentialTimeSeries();
        this.completed = new DifferentialTimeSeries();
        this.exceptions = new DifferentialTimeSeries();
    }

    public JobQueueMetrics(TimeSeries queued, TimeSeries failed,
                           TimeSeries completed, TimeSeries exceptions,
                           TimeSeries highJobs, TimeSeries midJobs,
                           TimeSeries lowJobs, TimeSeries futureJobs) {
        this.queued = queued;
        this.failed = failed;
        this.completed = completed;
        this.exceptions = exceptions;
        this.highJobs = highJobs;
        this.midJobs = midJobs;
        this.lowJobs = lowJobs;
        this.futureJobs = futureJobs;
    }

    public JobQueueMetrics compact() {
        return new JobQueueMetrics(
                compactTimeSeries(queued, SampleMethod.MEAN),
                compactTimeSeries(failed, SampleMethod.SUM),
                compactTimeSeries(completed, SampleMethod.SUM),
                compactTimeSeries(exceptions, SampleMethod.SUM),
                compactTimeSeries(highJobs, SampleMethod.MEAN),
                compactTimeSeries(midJobs, SampleMethod.MEAN),
                compactTimeSeries(lowJobs, SampleMethod.MEAN),
                compactTimeSeries(futureJobs, SampleMethod.MEAN)
        );
    }


    private TimeSeries compactTimeSeries(TimeSeries original, SampleMethod method) {
        TimeSeries ts = original.downSample(Frequency.of(5, TimeUnit.MINUTES),
                method);
        DateTime start = ts.index().last();
        DateTime end = original.index().last();

        if (start != null && end != null) {
            for (Map.Entry<DateTime, BigDecimal> entry : ts.dataWindow(start, end).entrySet()) {
                ts.add(entry.getKey(), entry.getValue());
            }
        }

        return ts;
    }
}

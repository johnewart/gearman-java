package net.johnewart.gearman.engine;


import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.engine.core.QueuedJob;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;

public class QueuedJobTest {
    @Test
    public void testHashCode() throws Exception
    {
        QueuedJob jobA = new QueuedJob("frobozz", -1, JobPriority.HIGH, "function");
        QueuedJob jobB = new QueuedJob("frobozz", 1024, JobPriority.HIGH, "function");

        Assert.assertThat("Hash codes represent a unique set of data points",
                jobA.hashCode() == jobB.hashCode(),
                Is.is(false));
    }

    @Test
    public void testEquality() throws Exception
    {
        QueuedJob jobA = new QueuedJob("frobozz", -1, JobPriority.HIGH, "function");
        QueuedJob jobB = new QueuedJob("frobozz", -1, JobPriority.HIGH, "function");

        Assert.assertThat("Queued Jobs are equal if their data points are the same",
                jobA.equals(jobB),
                Is.is(true));
    }
}

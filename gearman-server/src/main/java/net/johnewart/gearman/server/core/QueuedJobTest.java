package net.johnewart.gearman.server.core;


import net.johnewart.gearman.constants.JobPriority;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class QueuedJobTest {
    @Test
    public void testHashCode() throws Exception
    {
        QueuedJob jobA = new QueuedJob("frobozz", -1, JobPriority.HIGH, "function");
        QueuedJob jobB = new QueuedJob("frobozz", 1024, JobPriority.HIGH, "function");

        assertThat("Hash codes represent a unique set of data points",
                jobA.hashCode() == jobB.hashCode(),
                is(false));
    }

    @Test
    public void testEquality() throws Exception
    {
        QueuedJob jobA = new QueuedJob("frobozz", -1, JobPriority.HIGH, "function");
        QueuedJob jobB = new QueuedJob("frobozz", -1, JobPriority.HIGH, "function");

        assertThat("Queued Jobs are equal if their data points are the same",
                jobA.equals(jobB),
                is(true));
    }
}

package net.johnewart.gearman.engine;

import net.johnewart.gearman.engine.core.JobHandleFactory;
import net.johnewart.gearman.engine.util.LocalJobHandleFactory;
import org.hamcrest.core.Is;
import org.hamcrest.core.IsNot;
import org.junit.Assert;
import org.junit.Test;

public class JobHandleFactoryTest {
    private final JobHandleFactory jobHandleFactory;
    private final String hostname = "foobar.quiddle.com";

    public JobHandleFactoryTest() {
        jobHandleFactory = new LocalJobHandleFactory(hostname);
    }

    @Test
    public void testHandleGenerationWorks()
    {
        String jobHandle = new String(jobHandleFactory.getNextJobHandle());
        String[] parts = jobHandle.split(":");

        Assert.assertThat("There are three parts to the handle",
                parts.length,
                Is.is(3));

        Assert.assertThat("The first part is 'H'",
                parts[0],
                Is.is("H"));

        Assert.assertThat("The middle is the hostname",
                parts[1],
                Is.is("foobar.quiddle.com"));

        Assert.assertThat("The last part is non-zero",
                Long.parseLong(parts[2]),
                IsNot.not(0L));
    }



}

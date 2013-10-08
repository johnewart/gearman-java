package net.johnewart.gearman.engine;

import net.johnewart.gearman.engine.util.JobHandleFactory;
import org.hamcrest.core.Is;
import org.hamcrest.core.IsNot;
import org.junit.Assert;
import org.junit.Test;

public class JobHandleFactoryTest {
    @Test
    public void testHandleGenerationWorks()
    {
        JobHandleFactory.setHostName("foobar.quiddle.com");
        String jobHandle = new String(JobHandleFactory.getNextJobHandle());
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

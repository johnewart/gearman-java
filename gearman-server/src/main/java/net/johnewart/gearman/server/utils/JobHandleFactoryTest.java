package net.johnewart.gearman.server.utils;

import net.johnewart.gearman.server.util.JobHandleFactory;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;

public class JobHandleFactoryTest {
    @Test
    public void testHandleGenerationWorks()
    {
        JobHandleFactory.setHostName("foobar.quiddle.com");
        String jobHandle = new String(JobHandleFactory.getNextJobHandle());
        String[] parts = jobHandle.split(":");

        assertThat("There are three parts to the handle",
                parts.length,
                is(3));

        assertThat("The first part is 'H'",
                parts[0],
                is("H"));

        assertThat("The middle is the hostname",
                parts[1],
                is("foobar.quiddle.com"));

        assertThat("The last part is non-zero",
                Long.parseLong(parts[2]),
                not(0L));
    }



}

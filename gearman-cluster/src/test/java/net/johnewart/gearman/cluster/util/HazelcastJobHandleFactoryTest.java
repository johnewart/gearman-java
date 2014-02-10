package net.johnewart.gearman.cluster.util;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HazelcastJobHandleFactoryTest {

    @Test
    public void shouldIncrementCounter() {

        IAtomicLong mockAtomicLong = mock(IAtomicLong.class);
        when(mockAtomicLong.incrementAndGet()).thenReturn(1L);

        HazelcastInstance mockHazelcast = mock(HazelcastInstance.class);
        when(mockHazelcast.getAtomicLong(anyString())).thenReturn(mockAtomicLong);

        HazelcastJobHandleFactory hazelcastJobHandleFactory = new HazelcastJobHandleFactory(mockHazelcast, "localhost");

        assertThat("Generates a proper job handle",
                new String(hazelcastJobHandleFactory.getNextJobHandle()),
                is("H:localhost:1"));
    }


}

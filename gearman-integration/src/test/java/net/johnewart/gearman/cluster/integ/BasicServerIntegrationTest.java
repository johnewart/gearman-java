package net.johnewart.gearman.cluster.integ;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import net.johnewart.gearman.client.NetworkGearmanClient;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.interfaces.GearmanClient;
import net.johnewart.gearman.common.interfaces.GearmanFunction;
import net.johnewart.gearman.exceptions.JobSubmissionException;
import net.johnewart.gearman.exceptions.WorkException;
import net.johnewart.gearman.net.Connection;
import net.johnewart.gearman.server.config.GearmanServerConfiguration;
import net.johnewart.gearman.server.config.ServerConfiguration;
import net.johnewart.gearman.server.net.ServerListener;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class BasicServerIntegrationTest {
    private final Logger LOG = LoggerFactory.getLogger(BasicServerIntegrationTest.class);

    int SERVER_ONE_PORT = 8888;
    String SERVER_ONE_HOSTNAME = "localhost";

    public BasicServerIntegrationTest() throws InterruptedException {
        GearmanServerConfiguration primaryConfig = new GearmanServerConfiguration();
        primaryConfig.setHostName(SERVER_ONE_HOSTNAME);
        primaryConfig.setPort(SERVER_ONE_PORT);

        ServerRunner runner = new ServerRunner(primaryConfig);
        Thread t1 = new Thread(runner);
        t1.start();

        LOG.debug("Sleeping for 5s to let things start up");
        Thread.sleep(5000);
        LOG.debug("Running tests...");
    }

    @Test
    public void testForegroundJob() throws InterruptedException, IOException, JobSubmissionException, WorkException {

        CountingReverseFunc reverseFunc = new CountingReverseFunc(10);
        Set<Connection> connectionSet = ImmutableSet.of(new Connection(SERVER_ONE_HOSTNAME, SERVER_ONE_PORT));
        ImmutableMap<String, GearmanFunction> functions = ImmutableMap.of("reverse", (GearmanFunction)reverseFunc);
        WorkerRunner workerRunner = new WorkerRunner(connectionSet, functions);
        Thread t3 = new Thread(workerRunner);
        t3.start();

        String[] jobStrings = {
            "HELLO",
            "FOO",
            "BAR",
            "ONE",
            "BYE",
            "SNARF",
            "BAZ",
            "GEARMAN",
            "FOOBAR",
            "BIGFOOT"
        };


        GearmanClient client = new NetworkGearmanClient("localhost", SERVER_ONE_PORT);

        for(int i = 0; i < 10; i++) {
            byte[] jobData = jobStrings[i].getBytes();
            byte[] reversedData = jobData.clone();
            ArrayUtils.reverse(reversedData);
            byte[] result = client.submitJob("reverse", jobData);
            Assert.assertArrayEquals(result, reversedData);
            LOG.debug(String.format("Result: %s", new String(result)));
        }

        assertThat(reverseFunc.isComplete(), is(true));
        workerRunner.stop();
        t3.join();
    }

    private class CountingReverseFunc implements GearmanFunction {

        private final int maxHits;
        private int hits;

        public CountingReverseFunc(int num) {
            this.maxHits = num;
            this.hits = 0;
        }


        public boolean isComplete() {
            return hits == maxHits;
        }

        @Override
        public byte[] process(Job job) {
            hits += 1;
            LOG.debug(String.format("Processing job '%s' with data '%s'", job.getFunctionName(), new String(job.getData())));
            byte[] data = job.getData().clone();
            ArrayUtils.reverse(data);
            return data;
        }
    }

    private class ServerRunner implements Runnable {

        private final ServerListener server;

        public ServerRunner(ServerConfiguration serverConfig) {
            server = new ServerListener(serverConfig);
        }

        @Override
        public void run() {
            server.start();
        }

    }


}

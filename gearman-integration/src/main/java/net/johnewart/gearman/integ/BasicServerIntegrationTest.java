package net.johnewart.gearman.integ;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import net.johnewart.gearman.client.NetworkGearmanClient;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.JobStatus;
import net.johnewart.gearman.common.events.GearmanClientEventListener;
import net.johnewart.gearman.common.events.WorkEvent;
import net.johnewart.gearman.common.interfaces.GearmanClient;
import net.johnewart.gearman.common.interfaces.GearmanFunction;
import net.johnewart.gearman.common.interfaces.GearmanWorker;
import net.johnewart.gearman.engine.queue.factories.MemoryJobQueueFactory;
import net.johnewart.gearman.exceptions.JobSubmissionException;
import net.johnewart.gearman.exceptions.WorkException;
import net.johnewart.gearman.net.Connection;
import net.johnewart.gearman.server.config.GearmanServerConfiguration;
import net.johnewart.gearman.server.config.ServerConfiguration;
import net.johnewart.gearman.server.net.ServerListener;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class BasicServerIntegrationTest {
    private final Logger LOG = LoggerFactory.getLogger(BasicServerIntegrationTest.class);

    public BasicServerIntegrationTest()  { }

    public void runTest() throws InterruptedException, IOException, JobSubmissionException, WorkException {
        int port = 8888;
        String hostname = "localhost";

        GearmanServerConfiguration primaryConfig = new GearmanServerConfiguration();
        primaryConfig.setHostName(hostname);
        primaryConfig.setPort(port);
        primaryConfig.setJobQueueFactory(new MemoryJobQueueFactory(primaryConfig.getMetricRegistry()));

        ServerRunner runner = new ServerRunner(primaryConfig);
        Thread t1 = new Thread(runner);
        t1.start();

        LOG.debug("Sleeping for 5s to let things start up");
        Thread.sleep(5000);
        LOG.debug("Running tests...");


        CountingReverseFunc reverseFunc = new CountingReverseFunc(10);
        Set<Connection> connectionSet = ImmutableSet.of(new Connection(hostname, port));
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

        final AtomicInteger integer = new AtomicInteger(0);

        GearmanClient client = new NetworkGearmanClient("localhost", port);
        GearmanClientEventListener listener = new GearmanClientEventListener() {
            @Override
            public void handleWorkData(String jobHandle, byte[] data) {
                LOG.debug("handleWorkData for " + jobHandle);
            }

            @Override
            public void handleWorkWarning(String jobHandle, byte[] warning) {
                LOG.debug ("handleWorkWarning for " + jobHandle);
            }

            @Override
            public void handleWorkStatus(String jobHandle, JobStatus jobStatus) {
                LOG.debug ("handleWorkStatus for " + jobHandle);
                integer.addAndGet(1);
            }
        };

        client.registerEventListener(listener);


        for(int i = 0; i < 10; i++) {
            byte[] jobData = jobStrings[i].getBytes();
            byte[] reversedData = jobData.clone();
            ArrayUtils.reverse(reversedData);
            byte[] result = client.submitJob("reverse", jobData);
            Assert.assertArrayEquals(result, reversedData);
            LOG.debug(String.format("Result: %s", new String(result)));
        }



        assertThat(reverseFunc.isComplete(), is(true));
        assertEquals(integer.get(), 100);
        client.shutdown();
        workerRunner.stop();
        LOG.debug("Stopping worker thread");
        t3.join();
        LOG.debug("Stopping server thread");
        runner.stop();
        t1.join();
        LOG.debug("Done!");
    }

    public static void main(String... args) throws InterruptedException, IOException, JobSubmissionException, WorkException {
        BasicServerIntegrationTest basicServerIntegrationTest = new BasicServerIntegrationTest();
        basicServerIntegrationTest.runTest();
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
        public byte[] process(WorkEvent workEvent) {
            Job job = workEvent.job;
            GearmanWorker worker = workEvent.worker;

            hits += 1;
            LOG.debug(String.format("Processing job '%s' with data '%s'", job.getFunctionName(), new String(job.getData())));
            byte[] data = job.getData().clone();
            for(int i = 0; i < 10; i++) {
                try {
                    worker.sendStatus(job, i, 10);
                } catch (IOException e) {
                    LOG.error("Unable to send status: ", e);
                }
            }
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

        public void stop() {
            server.stop();
        }

    }


}

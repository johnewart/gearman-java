package net.johnewart.gearman.integ;

import net.johnewart.gearman.client.NetworkGearmanClient;
import net.johnewart.gearman.client.NetworkGearmanWorker;
import net.johnewart.gearman.cluster.config.ClusterConfiguration;
import net.johnewart.gearman.common.interfaces.GearmanClient;
import net.johnewart.gearman.common.interfaces.GearmanWorker;
import net.johnewart.gearman.exceptions.JobSubmissionException;
import net.johnewart.gearman.exceptions.WorkException;
import net.johnewart.gearman.server.net.ServerListener;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class ClusterIntegrationTest {
    private final Logger LOG = LoggerFactory.getLogger(ClusterIntegrationTest.class);

    int SERVER_ONE_PORT = 8888;
    String SERVER_ONE_HOSTNAME = "node1";
    int SERVER_TWO_PORT = 8889;
    String SERVER_TWO_HOSTNAME = "node2";

    public ClusterIntegrationTest() throws InterruptedException {
        ClusterConfiguration primaryConfig = new ClusterConfiguration();
        primaryConfig.setHostName(SERVER_ONE_HOSTNAME);
        primaryConfig.setPort(SERVER_ONE_PORT);
        ServerListener serverOne = new ServerListener(primaryConfig);
        Thread t1 = new Thread(new ServerRunner(serverOne));

        ClusterConfiguration secondaryConfig = new ClusterConfiguration();
        secondaryConfig.setHostName(SERVER_TWO_HOSTNAME);
        secondaryConfig.setPort(SERVER_TWO_PORT);
        ServerListener serverTwo = new ServerListener(secondaryConfig);
        Thread t2 = new Thread(new ServerRunner(serverTwo));

        t1.start();
        t2.start();

        LOG.debug("Sleeping for 10s to let things start up");
        Thread.sleep(10000);
        LOG.debug("Running tests...");

        try {
            testTwoNodeHazelcastCluster();
        } catch (IOException | JobSubmissionException | WorkException e) {
            e.printStackTrace();
        }

    }

    public static void main(String... args) {
        try {
            new ClusterIntegrationTest();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void testTwoNodeHazelcastCluster() throws InterruptedException, IOException, JobSubmissionException, WorkException {
        WorkerRunner workerRunner = new WorkerRunner();
        Thread t3 = new Thread(workerRunner);
        t3.start();

        byte[] jobData = {'H','E','L','L','O'};
        byte[] reversedData = jobData.clone();
        ArrayUtils.reverse(reversedData);

        GearmanClient client = new NetworkGearmanClient("localhost", SERVER_TWO_PORT);
        byte[] result = client.submitJob("reverse", jobData);

        Assert.assertArrayEquals(result, reversedData);
        LOG.debug(String.format("Result: %s", new String(result)));

        workerRunner.stop();
        t3.join();
    }

    private class ServerRunner implements Runnable {
        private final ServerListener serverListener;

        public ServerRunner(ServerListener serverListener) {
            this.serverListener = serverListener;
        }

        @Override
        public void run() {
            serverListener.start();
        }
    }

    private class WorkerRunner implements Runnable {
        private GearmanWorker worker;

        @Override
        public void run() {
            worker = new NetworkGearmanWorker.Builder()
                    .withHostPort("localhost", SERVER_ONE_PORT)
                    .build();

            worker.registerCallback("reverse", new ReverseFunc());
            worker.doWork();
        }

        public void stop() {
            worker.stopWork();
        }
    }

}

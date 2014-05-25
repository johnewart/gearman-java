package net.johnewart.gearman.example;

import net.johnewart.gearman.client.NetworkGearmanWorkerPool;
import net.johnewart.gearman.common.events.WorkEvent;
import net.johnewart.gearman.common.interfaces.GearmanFunction;
import net.johnewart.gearman.net.Connection;
import org.apache.commons.lang3.ArrayUtils;
import net.johnewart.gearman.common.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerPoolDemo {
    private static Logger LOG = LoggerFactory.getLogger(WorkerPoolDemo.class);

    static class ReverseFunction implements GearmanFunction
    {
        @Override
        public byte[] process(WorkEvent workEvent) {
            Job job = workEvent.job;
            byte[] data = job.getData();
            String function = job.getFunctionName();
            LOG.debug("Got data for function " + function);
            ArrayUtils.reverse(data);
            return data;
        }
    }

    public static void main(String... args)
    {
        try {
            byte data[] = "This is a test".getBytes();
            NetworkGearmanWorkerPool workerPool = new NetworkGearmanWorkerPool.Builder()
                                        .threads(2)
                                        .withConnection(new Connection("localhost", 4730))
                                        .build();

            workerPool.registerCallback("reverse", new ReverseFunction());

            workerPool.doWork();
        } catch (Exception e) {
            LOG.error("Error: ", e);
        }
    }
}

package net.johnewart.gearman.example;

import net.johnewart.gearman.client.NetworkGearmanWorker;
import org.apache.commons.lang3.ArrayUtils;
import net.johnewart.gearman.common.interfaces.GearmanFunction;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.net.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerDemo {
    private static Logger LOG = LoggerFactory.getLogger(WorkerDemo.class);

    static class ReverseFunction implements GearmanFunction
    {
        @Override
        public byte[] process(Job job) {
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
            NetworkGearmanWorker worker = new NetworkGearmanWorker.Builder()
                                        .withConnection(new Connection("localhost", 4730))
                                        .build();

            worker.registerCallback("reverse", new ReverseFunction());

            worker.doWork();
        } catch (Exception e) {
            LOG.error("oops!");
        }
    }
}

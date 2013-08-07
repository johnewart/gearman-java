package org.gearman.example;

import org.apache.commons.lang3.ArrayUtils;
import org.gearman.client.GearmanClient;
import org.gearman.client.GearmanFunction;
import org.gearman.client.GearmanWorker;
import org.gearman.common.Job;
import org.gearman.constants.JobPriority;
import org.gearman.exceptions.NoServersAvailableException;
import org.gearman.net.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
            GearmanWorker worker = new GearmanWorker.Builder()
                                        .withConnection(new Connection("localhost", 4730))
                                        .build();

            worker.registerCallback("reverse", new ReverseFunction());

            worker.doWork();
        } catch (Exception e) {
            LOG.error("oops!");
        }
    }
}

package net.johnewart.gearman.integ;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.events.WorkEvent;
import net.johnewart.gearman.common.interfaces.GearmanFunction;
import net.johnewart.gearman.common.interfaces.GearmanWorker;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ReverseFunc implements GearmanFunction {
    private final Logger LOG = LoggerFactory.getLogger(ReverseFunc.class);

    @Override
    public byte[] process(WorkEvent workEvent) {
        final Job job = workEvent.job;
        final GearmanWorker worker = workEvent.worker;

        LOG.debug(String.format("Processing job '%s' with data '%s'", job.getFunctionName(), new String(job.getData())));
        byte[] data = job.getData().clone();
        ArrayUtils.reverse(data);
        for(int i = 0; i < 10; i++) {
            try {
                worker.sendStatus(job, i, 10);
            } catch (IOException e) {
                LOG.error("Unable to send status: ", e);
            }
        }
        return data;
    }
}
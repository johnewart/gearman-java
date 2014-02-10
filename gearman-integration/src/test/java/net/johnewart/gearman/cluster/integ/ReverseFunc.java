package net.johnewart.gearman.cluster.integ;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.interfaces.GearmanFunction;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReverseFunc implements GearmanFunction {
    private final Logger LOG = LoggerFactory.getLogger(ReverseFunc.class);

    @Override
    public byte[] process(Job job) {
        LOG.debug(String.format("Processing job '%s' with data '%s'", job.getFunctionName(), new String(job.getData())));
        byte[] data = job.getData().clone();
        ArrayUtils.reverse(data);
        return data;
    }
}
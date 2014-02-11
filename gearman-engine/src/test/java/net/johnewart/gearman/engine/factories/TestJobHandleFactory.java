package net.johnewart.gearman.engine.factories;

import net.johnewart.gearman.engine.core.JobHandleFactory;
import net.johnewart.gearman.engine.util.LocalJobHandleFactory;

public class TestJobHandleFactory implements JobHandleFactory {
    private JobHandleFactory jobHandleFactory;

    public TestJobHandleFactory() {
        this.jobHandleFactory = new LocalJobHandleFactory("localhost");
    }

    @Override
    public byte[] getNextJobHandle() {
        return jobHandleFactory.getNextJobHandle();
    }
}

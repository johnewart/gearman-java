package net.johnewart.gearman.common.events;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.interfaces.GearmanWorker;

public class WorkEvent {
    public final Job job;
    public final GearmanWorker worker;

    public WorkEvent(final Job job, final GearmanWorker worker) {
        this.job = job;
        this.worker = worker;
    }
}

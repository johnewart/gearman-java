package net.johnewart.gearman.common.events;

import net.johnewart.gearman.common.JobStatus;

public interface GearmanClientEventListener {
    void handleWorkData(final String jobHandle, final byte[] data);
    void handleWorkWarning(final String jobHandle, final byte[] warning);
    void handleWorkStatus(final String jobHandle, JobStatus jobStatus);
}

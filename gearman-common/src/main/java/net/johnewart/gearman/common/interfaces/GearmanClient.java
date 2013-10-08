package net.johnewart.gearman.common.interfaces;


import net.johnewart.gearman.common.JobStatus;
import net.johnewart.gearman.common.events.GearmanClientEventListener;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.exceptions.JobSubmissionException;
import net.johnewart.gearman.exceptions.WorkException;

import java.util.Date;

public interface GearmanClient {
    String submitFutureJob(String callback, byte[] data, Date whenToRun) throws JobSubmissionException;
    String submitJobInBackground(String callback, byte[] data) throws JobSubmissionException;
    String submitJobInBackground(String callback, byte[] data, JobPriority priority) throws JobSubmissionException;
    byte[] submitJob(String callback, byte[] data) throws JobSubmissionException, WorkException;
    byte[] submitJob(String callback, byte[] data, JobPriority priority) throws JobSubmissionException, WorkException;
    JobStatus getStatus(String jobHandle);
    void registerEventListener(GearmanClientEventListener listener);
}

package org.gearman.common;

import org.gearman.common.packets.request.SetClientId;
import org.gearman.common.packets.request.SubmitJob;
import org.gearman.common.packets.response.JobAssignUniq;
import org.gearman.common.packets.response.WorkExceptionResponse;
import org.gearman.common.packets.response.WorkFailResponse;
import org.gearman.common.packets.response.WorkStatus;
import org.gearman.constants.JobPriority;
import org.gearman.constants.PacketType;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class PacketTest {
    @Test
    public void buildWorkFailedFromJobHandle() throws Exception {
        byte[] byteArray = {0, 'R','E','S',0,0,0,14,0,0,0,5,'j','o','b',':','1'};
        String jobHandle = "job:1";

        WorkFailResponse workFailResponse = new WorkFailResponse(jobHandle);

        assertThat("The jobhandle is job:1",
                workFailResponse.getJobHandle(),
                is("job:1"));

        assertThat("The packet type is PacketType.WORK_FAIL",
                workFailResponse.getType(),
                is(PacketType.WORK_FAIL));

        assertThat("The byte array generated is the one used to build it",
                workFailResponse.toByteArray(),
                is(byteArray));

    }

    @Test
    public void buildWorkStatusFromJobStatus() throws Exception {
        byte[] byteArray = {0, 'R','E','S',0,0,0,12,0,0,0,10,'j','o','b',':','1',0,'1',0,'1','0'};
        String jobHandle = "job:1";
        String numerator = "1";
        String denominator = "10";

        JobStatus jobStatus = new JobStatus(Integer.parseInt(numerator),
                                            Integer.parseInt(denominator),
                                            JobState.WORKING,
                                            jobHandle);

        WorkStatus workStatus = new WorkStatus(jobStatus);

        assertThat("The jobhandle is job:1",
                workStatus.getJobHandle(),
                is("job:1"));

        assertThat("The packet type is PacketType.WORK_FAIL",
                workStatus.getType(),
                is(PacketType.WORK_STATUS));

        assertThat("The byte array generated is the one used to build it",
                workStatus.toByteArray(),
                is(byteArray));

        assertThat("The packet is 22 bytes in length",
                workStatus.getSize(),
                is(22));

    }

    @Test
    public void buildWorkExceptionResponseFromJobHandleAndException() throws Exception {
        byte[] byteArray = {0, 'R','E','S',0,0,0,25,0,0,0,11,'j','o','b',':','1', 0, 'e','r','r','o','r' };
        String jobHandle = "job:1";
        byte[] exceptionData = {'e','r','r','o','r'};

        WorkExceptionResponse workExceptionResponse
                = new WorkExceptionResponse(jobHandle, exceptionData);

        assertThat("The jobhandle is job:1",
                workExceptionResponse.getJobHandle(),
                is("job:1"));

        assertThat("The packet type is PacketType.WORK_FAIL",
                workExceptionResponse.getType(),
                is(PacketType.WORK_EXCEPTION));

        assertThat("The byte array generated is the one used to build it",
                workExceptionResponse.toByteArray(),
                is(byteArray));

        assertThat("The exception byte data matches the input",
                workExceptionResponse.getException(),
                is(exceptionData));

    }

    @Test
    public void buildJobAssignUniqFromAllArguments() throws Exception
    {
        String jobHandle = "job:1";
        String funcName = "func";
        String uniqueId = "decaf";
        byte[] data = {'c','o','f','f','e','e'};
        byte[] byteArray = {0, 'R','E','S',0,0,0,31,0,0,0,23,'j','o','b',':','1',0,'f','u','n','c',0,'d','e','c','a','f',0,'c','o','f','f','e','e' };

        JobAssignUniq jobAssignUniq =
                new JobAssignUniq(jobHandle, funcName, uniqueId, data);

        assertThat("The jobhandle is job:1",
                jobAssignUniq.getJobHandle(),
                is("job:1"));

        assertThat("The packet type is PacketType.JOB_ASSIGN_UNIQ",
                jobAssignUniq.getType(),
                is(PacketType.JOB_ASSIGN_UNIQ));

        assertThat("The byte array generated is the one used to build it",
                jobAssignUniq.toByteArray(),
                is(byteArray));


    }

    @Test
    public void testSubmitJobConstructors() throws Exception
    {
        byte[] jobData = {'d','a','t','a'};
        SubmitJob submitJob = new SubmitJob("function", "unique_id", jobData, false);
        assertThat("The packet type is SUBMIT_JOB",
                   submitJob.getType(),
        is(PacketType.SUBMIT_JOB));

        submitJob = new SubmitJob("function", "unique_id", jobData, true);
        assertThat("The packet type is SUBMIT_JOB_BG",
                   submitJob.getType(),
        is(PacketType.SUBMIT_JOB_BG));

        submitJob = new SubmitJob("function", "unique_id", jobData, false, JobPriority.HIGH);
        assertThat("The packet type is SUBMIT_JOB_HIGH",
                   submitJob.getType(),
        is(PacketType.SUBMIT_JOB_HIGH));

        submitJob = new SubmitJob("function", "unique_id", jobData, true, JobPriority.HIGH);
        assertThat("The packet type is SUBMIT_JOB_HIGH_BG",
                   submitJob.getType(),
        is(PacketType.SUBMIT_JOB_HIGH_BG));

        assertThat("The priority is HIGH",
                   submitJob.getPriority(),
        is(JobPriority.HIGH));

        submitJob = new SubmitJob("function", "unique_id", jobData, false, JobPriority.LOW);
        assertThat("The packet type is SUBMIT_JOB_LOW",
                   submitJob.getType(),
        is(PacketType.SUBMIT_JOB_LOW));

        submitJob = new SubmitJob("function", "unique_id", jobData, true, JobPriority.LOW);
        assertThat("The packet type is SUBMIT_JOB_LOW_BG",
                   submitJob.getType(),
        is(PacketType.SUBMIT_JOB_LOW_BG));

        assertThat("The priority is LOW",
                   submitJob.getPriority(),
        is(JobPriority.LOW));
    }

    @Test
    public void testSetClientIdConstructor()
    {
        SetClientId setClientId = new SetClientId();
        assertThat("It is of type SET_CLIENT_ID",
                setClientId.getType(),
                is(PacketType.SET_CLIENT_ID));

        assertNotNull(setClientId.getClientId());
        assertThat("The client id length is > 0",
                setClientId.getClientId().length(),
                not(0));
    }
}

package org.gearman.common;

import org.gearman.common.packets.PacketFactory;
import org.gearman.common.packets.request.*;
import org.gearman.common.packets.response.*;
import org.gearman.constants.JobPriority;
import org.gearman.constants.PacketType;
import org.junit.Test;

import java.util.Date;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class PacketFactoryTest {
    @Test
    public void buildsJobCreatedPacket() throws Exception {
        byte[] jobCreatedArray = {0, 'R','E','S',0,0,0,8,0,0,0,5,'j','o','b',':','1'};
        JobCreated jobCreated = (JobCreated)PacketFactory.packetFromBytes(jobCreatedArray);

        assertThat("The jobhandle is job:1",
                jobCreated.getJobHandle(),
                is("job:1"));

        assertThat("The packet type is PacketType.JOB_CREATED",
                jobCreated.getType(),
                is(PacketType.JOB_CREATED));

        assertThat("The byte array generated is the one used to build it",
                jobCreated.toByteArray(),
                is(jobCreatedArray));
    }



    @Test
    public void buildsJobAssignPacket() throws Exception {
        byte[] jobAssignData = {0, 'R','E','S',0,0,0,11,0,0,0,13,'j','o','b',':','2',0,'f','o','o',0,'b','a','r'};
        byte[] jobAssignDataBytes = {'b','a','r'};
        JobAssign jobAssign = (JobAssign)PacketFactory.packetFromBytes(jobAssignData);

        assertThat("The jobhandle is job:2",
                jobAssign.getJobHandle(),
                is("job:2"));

        assertThat("The function name is 'foo'",
                jobAssign.getFunctionName(),
                is("foo"));


        assertThat("The job data is ['f','o'.'o']",
                jobAssign.getData(),
                is(jobAssignDataBytes));

        assertThat("The packet type is PacketType.JOB_ASSIGN",
                jobAssign.getType(),
                is(PacketType.JOB_ASSIGN));

        assertThat("The byte array generated is the one used to build it",
                jobAssign.toByteArray(),
                is(jobAssignData));


    }

    @Test
    public void buildsJobAssignUniqPacket() throws Exception {
        byte[] jobAssignData = {0, 'R','E','S',0,0,0,31,0,0,0,17,'j','o','b',':','2',0,'f','o','o',0,'1','2','3',0,'b','a','r'};
        byte[] jobAssignDataBytes = {'b','a','r'};

        JobAssignUniq jobAssignUniq = (JobAssignUniq)PacketFactory.packetFromBytes(jobAssignData);

        assertThat("The jobhandle is job:2",
                jobAssignUniq.getJobHandle(),
                is("job:2"));

        assertThat("The function name is 'foo'",
                jobAssignUniq.getFunctionName(),
                is("foo"));

        assertThat("The unique id is '123'",
                jobAssignUniq.getUniqueId(),
                is("123"));


        assertThat("The job data is ['f','o'.'o']",
                jobAssignUniq.getData(),
                is(jobAssignDataBytes));

        assertThat("The packet type is PacketType.JOB_ASSIGN_UNIQ",
                jobAssignUniq.getType(),
                is(PacketType.JOB_ASSIGN_UNIQ));

        assertThat("The byte array generated is the one used to build it",
                jobAssignUniq.toByteArray(),
                is(jobAssignData));

    }

    @Test
    public void buildsNoJobPacket() throws Exception {
        byte[] byteArray = {0, 'R','E','S',0,0,0,6,0,0,0,0};

        NoOp noOpPacket = (NoOp)PacketFactory.packetFromBytes(byteArray);

        assertThat("The packet type is PacketType.NOOP",
                noOpPacket.getType(),
                is(PacketType.NOOP));

        assertThat("The byte array generated is the one used to build it",
                noOpPacket.toByteArray(),
                is(byteArray));
    }

    @Test
    public void buildsWorkDataResponsePacket() throws Exception {
        byte[] byteArray = {0, 'R','E','S',0,0,0,28,0,0,0,9,'j','o','b',':','1',0,'f','o','o'};

        WorkDataResponse workDataResponsePacket = (WorkDataResponse)PacketFactory.packetFromBytes(byteArray);

        assertThat("The jobhandle is job:1",
                workDataResponsePacket.getJobHandle(),
                is("job:1"));

        assertThat("The packet type is PacketType.JOB_ASSIGN_UNIQ",
                workDataResponsePacket.getType(),
                is(PacketType.WORK_DATA));

        assertThat("The byte array generated is the one used to build it",
                workDataResponsePacket.toByteArray(),
                is(byteArray));

    }

    @Test
    public void buildsWorkFailedResponsePacket() throws Exception {
        byte[] byteArray = {0, 'R','E','S',0,0,0,14,0,0,0,5,'j','o','b',':','1'};

        WorkFailResponse workFailResponse =
                (WorkFailResponse)PacketFactory.packetFromBytes(byteArray);

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
    public void buildsWorkCompleteResponsePacket() throws Exception {
        byte[] byteArray = {0, 'R','E','S',0,0,0,13,0,0,0,9,'j','o','b',':','1',0,'f','o','o'};
        byte[] workData = {'f', 'o', 'o'};

        WorkCompleteResponse workCompleteResponse =
                (WorkCompleteResponse)PacketFactory.packetFromBytes(byteArray);

        assertThat("The jobhandle is job:1",
                workCompleteResponse.getJobHandle(),
                is("job:1"));

        assertThat("The work complete data is 'f', 'o', 'o'",
                workCompleteResponse.getData(),
                is(workData));

        assertThat("The packet type is PacketType.WORK_COMPLETE",
                workCompleteResponse.getType(),
                is(PacketType.WORK_COMPLETE));

        assertThat("The byte array generated is the one used to build it",
                workCompleteResponse.toByteArray(),
                is(byteArray));

    }

    @Test
    public void buildsWorkExceptionResponsePacket() throws Exception {
        byte[] byteArray = {0, 'R','E','S',0,0,0,25,0,0,0,11,'j','o','b',':','1',0,'e','r','r','o','r'};
        byte[] exceptionData = {'e', 'r', 'r','o','r'};

        WorkExceptionResponse workExceptionResponse =
                (WorkExceptionResponse)PacketFactory.packetFromBytes(byteArray);

        assertThat("The jobhandle is job:1",
                workExceptionResponse.getJobHandle(),
                is("job:1"));

        assertThat("The work exception is 'e', 'r', 'r', 'o', 'r'",
                workExceptionResponse.getException(),
                is(exceptionData));

        assertThat("The packet type is PacketType.WORK_EXCEPTION",
                workExceptionResponse.getType(),
                is(PacketType.WORK_EXCEPTION));

        assertThat("The byte array generated is the one used to build it",
                workExceptionResponse.toByteArray(),
                is(byteArray));

    }

    @Test
    public void buildsNoJobResponsePacket() throws Exception {
        byte[] byteArray = {0, 'R','E','S',0,0,0,10,0,0,0,0};

        NoJob noJobPacket =
                (NoJob)PacketFactory.packetFromBytes(byteArray);

        assertThat("The packet type is PacketType.NO_JOB",
                noJobPacket.getType(),
                is(PacketType.NO_JOB));

        assertThat("The byte array generated is the one used to build it",
                noJobPacket.toByteArray(),
                is(byteArray));

    }



    @Test
    public void buildsStatusResponsePacketForRunningJob() throws Exception {
        byte[] byteArray = {0, 'R','E','S',0,0,0,20,0,0,0,16,'r','e','s',':','1',0,'1',0,'1',0,'5','0',0,'1','0','0'};

        StatusRes statusResPacket =
                (StatusRes)PacketFactory.packetFromBytes(byteArray);

        assertThat("The job handle is 'res:1'",
                statusResPacket.getJobHandle(),
                is("res:1"));

        assertThat("The status is known",
                statusResPacket.isStatusKnown(),
                is(true));

        assertThat("The job is running",
                statusResPacket.isRunning(),
                is(true));

        assertThat("The numerator is 50",
                statusResPacket.getNumerator(),
                is(50));

        assertThat("The denominator is 100",
                statusResPacket.getDenominator(),
                is(100));

        assertThat("The percentage complete is .50",
                statusResPacket.percentComplete(),
                is(0.50f));

        assertThat("The packet type is PacketType.STATUS_RES",
                statusResPacket.getType(),
                is(PacketType.STATUS_RES));

        assertThat("The byte array generated is the one used to build it",
                statusResPacket.toByteArray(),
                is(byteArray));

    }

    @Test
    public void buildsStatusResponsePacketForNonRunningJob() throws Exception {
        byte[] byteArray = {0, 'R','E','S',0,0,0,20,0,0,0,16,'r','e','s',':','1',0,'1',0,'0',0,'5','0',0,'1','0','0'};

        StatusRes statusResPacket =
                (StatusRes)PacketFactory.packetFromBytes(byteArray);

        assertThat("The status is known",
                statusResPacket.isStatusKnown(),
                is(true));

        assertThat("The job is not running",
                statusResPacket.isRunning(),
                is(false));

        assertThat("The byte array generated is the one used to build it",
                statusResPacket.toByteArray(),
                is(byteArray));

    }

    @Test
    public void buildsStatusResponsePacketForRunningJobWhoseStatusIsUnknown() throws Exception {
        byte[] byteArray = {0, 'R','E','S',0,0,0,20,0,0,0,13,'r','e','s',':','1',0,'0',0,'1',0,'0',0,'0'};

        StatusRes statusResPacket =
                (StatusRes)PacketFactory.packetFromBytes(byteArray);

        assertThat("The status is unknown",
                statusResPacket.isStatusKnown(),
                is(false));

        assertThat("The job is running",
                statusResPacket.isRunning(),
                is(true));

        assertThat("The byte array generated is the one used to build it",
                statusResPacket.toByteArray(),
                is(byteArray));
    }

    /** Request packets **/
    @Test
    public void buildsCanDoRequestPacket() throws Exception {
        byte[] byteArray = {0, 'R','E','Q',0,0,0,1,0,0,0,4,'w','o','r','k'};

        CanDo canDo =
                (CanDo)PacketFactory.packetFromBytes(byteArray);

        assertThat("The packet type is CAN_DO",
                canDo.getType(),
                is(PacketType.CAN_DO));

        assertThat("The byte array generated is the one used to build it",
                canDo.toByteArray(),
                is(byteArray));

        assertThat("The payload length is 4",
                canDo.getPayloadSize(),
                is(4));

    }

    @Test
    public void buildsCantDoRequestPacket() throws Exception {
        byte[] byteArray = {0, 'R','E','Q',0,0,0,2,0,0,0,4,'w','o','r','k'};

        CantDo cantDo =
                (CantDo)PacketFactory.packetFromBytes(byteArray);

        assertThat("The packet type is CANT_DO",
                cantDo.getType(),
                is(PacketType.CANT_DO));

        assertThat("The byte array generated is the one used to build it",
                cantDo.toByteArray(),
                is(byteArray));

        assertThat("The payload length is 4",
                cantDo.getPayloadSize(),
                is(4));

    }


    @Test
    public void buildsSetClientIdRequestPacket() throws Exception {
        byte[] byteArray = {0, 'R','E','Q',0,0,0,22,0,0,0,6,'c','l','i','e','n','t'};

        SetClientId setClientId =
                (SetClientId)PacketFactory.packetFromBytes(byteArray);

        assertThat("The packet type is SET_CLIENT_ID",
                setClientId.getType(),
                is(PacketType.SET_CLIENT_ID));

        assertThat("The byte array generated is the one used to build it",
                setClientId.toByteArray(),
                is(byteArray));

        assertThat("The payload length is 6",
                setClientId.getPayloadSize(),
                is(6));
    }

    @Test
    public void buildsSubmitJobPackets() throws Exception
    {
        // SUBMIT_JOB
        byte[] byteArray = {0,'R','E','Q',
                            0,0,0,7,
                            0,0,0,11,
                            'f','o','o',0,'i','d',0,'d','a','t','a'};

        byte[] jobData = {'d','a','t','a'};

        SubmitJob submitJob =
                (SubmitJob)PacketFactory.packetFromBytes(byteArray);

        assertThat("The packet type is SUBMIT_JOB",
                submitJob.getType(),
                is(PacketType.SUBMIT_JOB));

        assertThat("The byte array generated is the one used to build it",
                submitJob.toByteArray(),
                is(byteArray));

        assertThat("The job data is 'data'",
                submitJob.getData(),
                is(jobData));

        assertThat("The job queue is 'foo'",
                submitJob.getFunctionName(),
                is("foo"));

        assertThat("The unique id is 'id'",
                submitJob.getUniqueId(),
                is("id"));

        assertThat("The priority is NORMAL",
                submitJob.getPriority(),
                is(JobPriority.NORMAL));

        byteArray[7] = 18; // SUBMIT_JOB_BG
        submitJob =
                (SubmitJob)PacketFactory.packetFromBytes(byteArray);

        assertThat("The packet type is SUBMIT_JOB_BG",
                submitJob.getType(),
                is(PacketType.SUBMIT_JOB_BG));

        assertThat("The byte array generated is the one used to build it",
                submitJob.toByteArray(),
                is(byteArray));

        assertThat("The priority is NORMAL",
                submitJob.getPriority(),
                is(JobPriority.NORMAL));


        byteArray[7] = 21; // SUBMIT_JOB_HIGH
        submitJob =
                (SubmitJob)PacketFactory.packetFromBytes(byteArray);

        assertThat("The packet type is SUBMIT_JOB_HIGH",
                submitJob.getType(),
                is(PacketType.SUBMIT_JOB_HIGH));

        assertThat("The byte array generated is the one used to build it",
                submitJob.toByteArray(),
                is(byteArray));

        assertThat("The priority is HIGH",
                submitJob.getPriority(),
                is(JobPriority.HIGH));


        byteArray[7] = 32; // SUBMIT_JOB_HIGH_BG
        submitJob =
                (SubmitJob)PacketFactory.packetFromBytes(byteArray);

        assertThat("The packet type is SUBMIT_JOB_HIGH_BG",
                submitJob.getType(),
                is(PacketType.SUBMIT_JOB_HIGH_BG));

        assertThat("The byte array generated is the one used to build it",
                submitJob.toByteArray(),
                is(byteArray));

        assertThat("The priority is HIGH",
                submitJob.getPriority(),
                is(JobPriority.HIGH));


        byteArray[7] = 33; // SUBMIT_JOB_LOW
        submitJob =
                (SubmitJob)PacketFactory.packetFromBytes(byteArray);

        assertThat("The packet type is SUBMIT_JOB_LOW",
                submitJob.getType(),
                is(PacketType.SUBMIT_JOB_LOW));

        assertThat("The byte array generated is the one used to build it",
                submitJob.toByteArray(),
                is(byteArray));

        assertThat("The priority is LOW",
                submitJob.getPriority(),
                is(JobPriority.LOW));


        byteArray[7] = 34; // SUBMIT_JOB_LOW_BG
        submitJob =
                (SubmitJob)PacketFactory.packetFromBytes(byteArray);

        assertThat("The packet type is SUBMIT_JOB_LOW_BG",
                submitJob.getType(),
                is(PacketType.SUBMIT_JOB_LOW_BG));

        assertThat("The priority is LOW",
                submitJob.getPriority(),
                is(JobPriority.LOW));

        assertThat("The byte array generated is the one used to build it",
                submitJob.toByteArray(),
                is(byteArray));

    }

    @Test
    public void buildsSubmitJobEpochPackets() throws Exception
    {
        // SUBMIT_JOB_EPOCH
        byte[] byteArray = {0,'R','E','Q',
                0,0,0,36,
                0,0,0,15,
                'f','o','o',0,'i','d',0,'1','0','0',0,'d','a','t','a'};

        byte[] jobData = {'d','a','t','a'};

        SubmitJob submitJob =
                (SubmitJob)PacketFactory.packetFromBytes(byteArray);

        assertThat("The packet type is SUBMIT_JOB_EPOCH",
                submitJob.getType(),
                is(PacketType.SUBMIT_JOB_EPOCH));

        assertThat("The byte array generated is the one used to build it",
                submitJob.toByteArray(),
                is(byteArray));

        assertThat("The job data is 'data'",
                submitJob.getData(),
                is(jobData));

        assertThat("The run time is 100 seconds after epoch",
                submitJob.getEpoch(),
                is(100L));

        assertThat("The resulting Date object is ",
                submitJob.getWhen(),
                is(new Date(100L)));

    }

    @Test
    public void buildsPreSleepPacket() throws Exception
    {
        byte[] byteArray = {0,'R','E','Q',
                0,0,0,4,
                0,0,0,0};

        PreSleep preSleep =
                (PreSleep)PacketFactory.packetFromBytes(byteArray);

        assertThat("The packet type is PRE_SLEEP",
                preSleep.getType(),
                is(PacketType.PRE_SLEEP));

        assertThat("The payload size is zero",
                preSleep.getPayloadSize(),
                is(0));
    }

    @Test
    public void buildsGrabJobPacket() throws  Exception
    {
        byte[] byteArray = {0,'R','E','Q',
                0,0,0,9,
                0,0,0,0};

        GrabJob grabJob =
                (GrabJob)PacketFactory.packetFromBytes(byteArray);

        assertThat("The packet type is GRAB_JOB",
                grabJob.getType(),
                is(PacketType.GRAB_JOB));

        assertThat("The payload size is zero",
                grabJob.getPayloadSize(),
                is(0));
    }

}

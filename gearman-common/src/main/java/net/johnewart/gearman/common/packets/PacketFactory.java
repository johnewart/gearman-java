package net.johnewart.gearman.common.packets;

import com.google.common.primitives.Ints;
import net.johnewart.gearman.common.packets.request.*;
import net.johnewart.gearman.common.packets.response.EchoResponse;
import net.johnewart.gearman.common.packets.response.JobAssign;
import net.johnewart.gearman.common.packets.response.JobAssignUniq;
import net.johnewart.gearman.common.packets.response.JobCreated;
import net.johnewart.gearman.common.packets.response.NoJob;
import net.johnewart.gearman.common.packets.response.NoOp;
import net.johnewart.gearman.common.packets.response.StatusRes;
import net.johnewart.gearman.common.packets.response.WorkCompleteResponse;
import net.johnewart.gearman.common.packets.response.WorkDataResponse;
import net.johnewart.gearman.common.packets.response.WorkExceptionResponse;
import net.johnewart.gearman.common.packets.response.WorkFailResponse;
import net.johnewart.gearman.common.packets.response.WorkStatus;
import net.johnewart.gearman.common.packets.response.WorkWarningResponse;
import net.johnewart.gearman.constants.PacketType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class PacketFactory {
    private static final Logger LOG = LoggerFactory.getLogger(PacketFactory.class);

    public static Packet packetFromBytes(byte[] packetBytes)
    {
        byte[] sizebytes = Arrays.copyOfRange(packetBytes, 8, 12);
        byte[] typebytes = Arrays.copyOfRange(packetBytes, 4, 8);
        byte[] magicbytes = Arrays.copyOfRange(packetBytes, 0, 4);

        int messagesize = Ints.fromByteArray(sizebytes);
        int messagetype = Ints.fromByteArray(typebytes);

        PacketType packetType = PacketType.fromPacketMagicNumber(messagetype);

        switch(packetType)
        {
            case JOB_CREATED:
                return new JobCreated(packetBytes);
            case WORK_DATA:
                return new WorkDataResponse(packetBytes);
            case WORK_WARNING:
                return new WorkWarningResponse(packetBytes);
            case WORK_STATUS:
                return new WorkStatus(packetBytes);
            case WORK_COMPLETE:
                return new WorkCompleteResponse(packetBytes);
            case WORK_FAIL:
                return new WorkFailResponse(packetBytes);
            case WORK_EXCEPTION:
                return new WorkExceptionResponse(packetBytes);
            case STATUS_RES:
                return new StatusRes(packetBytes);
            case GET_STATUS:
                return new GetStatus(packetBytes);

            /* Worker response packets */
            case NOOP:
                return new NoOp();
            case NO_JOB:
                return new NoJob();
            case JOB_ASSIGN:
                return new JobAssign(packetBytes);
            case JOB_ASSIGN_UNIQ:
                return new JobAssignUniq(packetBytes);
            /* Worker request packets */
            case RESET_ABILITIES:
                return new ResetAbilities(packetBytes);
            case CAN_DO_TIMEOUT:
                return new CanDoTimeout(packetBytes);
            case CANT_DO:
                return new CantDo(packetBytes);
            case CAN_DO:
                return new CanDo(packetBytes);
            case GRAB_JOB:
                return new GrabJob(packetBytes);
            case GRAB_JOB_UNIQ:
                return new GrabJobUniq(packetBytes);
            case PRE_SLEEP:
                return new PreSleep(packetBytes);
            case SET_CLIENT_ID:
                return new SetClientId(packetBytes);

            /* Client request packets */
            case SUBMIT_JOB:
            case SUBMIT_JOB_HIGH:
            case SUBMIT_JOB_LOW:
            case SUBMIT_JOB_BG:
            case SUBMIT_JOB_HIGH_BG:
            case SUBMIT_JOB_LOW_BG:
            case SUBMIT_JOB_SCHED:
            case SUBMIT_JOB_EPOCH:
                return new SubmitJob(packetBytes);

            case ECHO_REQ:
                return new EchoRequest(packetBytes);

            case ECHO_RES:
                return new EchoResponse(packetBytes);

            /* TODO: Handle these. */
            case OPTION_RES:
            case ERROR:
            default:
                LOG.error("Unhandled type: ", messagetype);
                return null;
        }
    }
}

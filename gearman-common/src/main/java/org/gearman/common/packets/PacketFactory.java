package org.gearman.common.packets;

import com.google.common.primitives.Ints;
import org.gearman.common.packets.request.*;
import org.gearman.common.packets.response.*;
import org.gearman.constants.PacketType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 12/1/12
 * Time: 1:11 AM
 * To change this template use File | Settings | File Templates.
 */
public class PacketFactory {
    private static final Logger LOG = LoggerFactory.getLogger(PacketFactory.class);

    public static Packet packetFromBytes(byte[] packetBytes)
    {
        byte[] sizebytes = Arrays.copyOfRange(packetBytes, 8, 12);
        byte[] typebytes = Arrays.copyOfRange(packetBytes, 4, 8);

        int messagesize = Ints.fromByteArray(sizebytes);
        int messagetype = Ints.fromByteArray(typebytes);

        PacketType packetType = PacketType.fromPacketMagicNumber(messagetype);

        switch(packetType)
        {
            case JOB_CREATED:
                return new JobCreated(packetBytes);
            case WORK_DATA:
                return new WorkData(packetBytes);
            case WORK_WARNING:
                return new WorkWarning(packetBytes);
            case WORK_STATUS:
                return new WorkStatus(packetBytes);
            case WORK_COMPLETE:
                return new WorkComplete(packetBytes);
            case WORK_FAIL:
                return new WorkFail(packetBytes);
            case WORK_EXCEPTION:
                return new WorkException(packetBytes);
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
            case CANT_DO:
                return new CantDo(packetBytes);
            case CAN_DO:
                return new CanDo(packetBytes);
            case GRAB_JOB:
                return new GrabJob(packetBytes);
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

            /* TODO: Handle these. */
            case OPTION_RES:
            case ECHO_RES:
            case ERROR:
            default:
                LOG.error("Unhandled type: ", messagetype);
                return null;
        }
    }
}

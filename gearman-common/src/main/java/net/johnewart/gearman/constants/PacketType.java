package net.johnewart.gearman.constants;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 8:01 AM
 * To change this template use File | Settings | File Templates.
 */
public enum PacketType {
    CAN_DO,				    // REQ    Worker
    CANT_DO,				// REQ    Worker
    RESET_ABILITIES,		// REQ    Worker
    PRE_SLEEP,				// REQ    Worker
    NOTHING,				// -      -
    NOOP,					// RES    Worker
    SUBMIT_JOB,				// REQ    Client
    JOB_CREATED,			// RES    Client
    GRAB_JOB,				// REQ    Worker
    NO_JOB,					// RES    Worker
    JOB_ASSIGN,				// RES    Worker
    WORK_STATUS,			// REQ    Worker
                            // RES    Client
    WORK_COMPLETE,			// REQ    Worker
                            // RES    Client
    WORK_FAIL,				// REQ    Worker
                            // RES    Client
    GET_STATUS,				// REQ    Client
    ECHO_REQ,				// REQ    Client/Worker
    ECHO_RES,				// RES    Client/Worker
    SUBMIT_JOB_BG,			// REQ    Client
    ERROR,					// RES    Client/Worker
    STATUS_RES,				// RES    Client
    SUBMIT_JOB_HIGH,		// REQ    Client
    SET_CLIENT_ID,			// REQ    Worker
    CAN_DO_TIMEOUT,			// REQ    Worker
    ALL_YOURS,				// REQ    Worker
    WORK_EXCEPTION,			// REQ    Worker
                            // RES    Client
    OPTION_REQ,				// REQ    Client/Worker
    OPTION_RES,				// RES    Client/Worker
    WORK_DATA,				// REQ    Worker
                            // RES    Client
    WORK_WARNING,			// REQ    Worker
                            // RES    Client
    GRAB_JOB_UNIQ,			// REQ    Worker
    JOB_ASSIGN_UNIQ,		// RES    Worker
    SUBMIT_JOB_HIGH_BG,		// REQ    Client
    SUBMIT_JOB_LOW,			// REQ    Client
    SUBMIT_JOB_LOW_BG,		// REQ    Client
    SUBMIT_JOB_SCHED,		// REQ    Client
    SUBMIT_JOB_EPOCH,	    // REQ    Client
    SUBMIT_REDUCE_JOB,      // REQ    Client
    SUBMIT_REDUCE_JOB_BACKGROUND,
                            // REQ    Client
    GRAB_JOB_ALL,           // REQ    Worker
    JOB_ASSIGN_ALL,         // RES    Worker
    GET_STATUS_UNIQUE,      // REQ    Client
    STATUS_RES_UNIQUE;      // RES    Client

    public int getIndex() { return ordinal() + 1; }
    public static PacketType fromPacketMagicNumber(int x) {
        switch(x) {
            case 1:  return CAN_DO;
            case 2:  return CANT_DO;
            case 3:  return RESET_ABILITIES;
            case 4:  return PRE_SLEEP;
            case 5:  return NOTHING;
            case 6:  return NOOP;
            case 7:  return SUBMIT_JOB;
            case 8:  return JOB_CREATED;
            case 9:  return GRAB_JOB;
            case 10: return NO_JOB;
            case 11: return JOB_ASSIGN;
            case 12: return WORK_STATUS;
            case 13: return WORK_COMPLETE;
            case 14: return WORK_FAIL;
            case 15: return GET_STATUS;
            case 16: return ECHO_REQ;
            case 17: return ECHO_RES;
            case 18: return SUBMIT_JOB_BG;
            case 19: return ERROR;
            case 20: return STATUS_RES;
            case 21: return SUBMIT_JOB_HIGH;
            case 22: return SET_CLIENT_ID;
            case 23: return CAN_DO_TIMEOUT;
            case 24: return ALL_YOURS;
            case 25: return WORK_EXCEPTION;
            case 26: return OPTION_REQ;
            case 27: return OPTION_RES;
            case 28: return WORK_DATA;
            case 29: return WORK_WARNING;
            case 30: return GRAB_JOB_UNIQ;
            case 31: return JOB_ASSIGN_UNIQ;
            case 32: return SUBMIT_JOB_HIGH_BG;
            case 33: return SUBMIT_JOB_LOW;
            case 34: return SUBMIT_JOB_LOW_BG;
            case 35: return SUBMIT_JOB_SCHED;
            case 36: return SUBMIT_JOB_EPOCH;
            case 37: return SUBMIT_REDUCE_JOB;
            case 38: return SUBMIT_REDUCE_JOB_BACKGROUND;
            case 39: return GRAB_JOB_ALL;
            case 40: return JOB_ASSIGN_ALL;
            case 41: return GET_STATUS_UNIQUE;
            case 42: return STATUS_RES_UNIQUE;
        }
        return null;
    }
}

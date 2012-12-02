package org.gearman.common.packets.response;

import org.gearman.constants.PacketType;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 12/1/12
 * Time: 9:59 PM
 * To change this template use File | Settings | File Templates.
 */
public interface WorkResponse {
    public abstract String getJobHandle();
    public abstract PacketType getType();
}

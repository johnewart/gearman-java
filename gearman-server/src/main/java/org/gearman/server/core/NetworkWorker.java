package org.gearman.server.core;

import org.gearman.common.interfaces.Worker;
import org.gearman.common.packets.Packet;
import org.gearman.common.packets.response.NoOp;
import org.jboss.netty.channel.Channel;

import java.util.HashSet;
import java.util.Set;

public class NetworkWorker implements Worker {
    private final Channel channel;
    private final Set<String> abilities;

    public NetworkWorker(Channel channel)
    {
        this.channel = channel;
        this.abilities = new HashSet<>();
    }

    public void send(Packet packet)
    {
        channel.write(packet);
    }

    @Override
    public Set<String> getAbilities() {
        return abilities;
    }

    @Override
    public void addAbility(String ability)
    {
        this.abilities.add(ability);
    }

    @Override
    public void wakeUp()
    {
        channel.write(new NoOp());
    }

}


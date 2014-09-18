package net.johnewart.gearman.server.net;

import io.netty.channel.Channel;
import net.johnewart.gearman.common.interfaces.EngineWorker;
import net.johnewart.gearman.common.packets.Packet;
import net.johnewart.gearman.common.packets.response.NoOp;

import java.util.HashSet;
import java.util.Set;

public class NetworkEngineWorker implements EngineWorker {
    private final Channel channel;
    private final Set<String> abilities;

    public NetworkEngineWorker(Channel channel)
    {
        this.channel = channel;
        this.abilities = new HashSet<>();
    }

    public void send(Packet packet)
    {
        channel.writeAndFlush(packet);
    }

    public void addAbility(String ability)
    {
        this.abilities.add(ability);
    }

    @Override
    public Set<String> getAbilities() {
        return abilities;
    }


    @Override
    public void wakeUp()
    {
        send(new NoOp());
    }

    public void removeAbility(String functionName) {
        this.abilities.remove(functionName);
    }
}


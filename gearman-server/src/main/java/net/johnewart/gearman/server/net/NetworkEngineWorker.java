package net.johnewart.gearman.server.net;

import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;

import io.netty.channel.Channel;
import net.johnewart.gearman.common.interfaces.EngineWorker;
import net.johnewart.gearman.common.packets.Packet;
import net.johnewart.gearman.common.packets.response.NoOp;

public class NetworkEngineWorker implements EngineWorker {
	private final Channel channel;
	private final Set<String> abilities;
	private Boolean awake;

	public NetworkEngineWorker(Channel channel) {
		this.channel = channel;
		this.abilities = new HashSet<>();
		this.awake = new Boolean(true);
	}

	public void send(Packet packet) {
		channel.writeAndFlush(packet);
	}

	public void addAbility(String ability) {
		this.abilities.add(ability);
	}

	public SocketAddress getAddress() {
		return channel.remoteAddress();
	}

	public String getId() {
		return String.valueOf(channel.hashCode() & 0xffffffffL);
	}

	@Override
	public Set<String> getAbilities() {
		return abilities;
	}

	@Override
	public void wakeUp() {
		synchronized (awake) {
			if (!awake) {
				send(new NoOp());
				awake = true;
			}
		}
	}

	@Override
	public void markAsleep() {
		synchronized (awake) {
			awake = false;
		}
	}

	public void removeAbility(String functionName) {
		this.abilities.remove(functionName);
	}
}

package net.johnewart.gearman.server.cluster.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import net.johnewart.gearman.constants.PacketType;

import java.io.IOException;

public class WorkMessage implements DataSerializable {
    public String uniqueId, functionName;
    public PacketType type;
    public byte[] data;

    public WorkMessage() { }

    public WorkMessage(String uniqueId, String functionName, PacketType type, byte[] data) {
        this.uniqueId = uniqueId;
        this.functionName = functionName;
        this.type = type;
        if (data != null) {
            this.data = data.clone();
        } else {
            this.data = new byte[0];
        }
    }

    // TODO: Optimize.
    @Override
    public void writeData(ObjectDataOutput output) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        output.writeUTF(mapper.writeValueAsString(this));
    }

    @Override
    public void readData(ObjectDataInput input) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        WorkMessage message = mapper.readValue(input.readUTF(), this.getClass());
        this.uniqueId = message.uniqueId;
        this.functionName = message.functionName;
        this.type = message.type;
        this.data = message.data.clone();
    }
}

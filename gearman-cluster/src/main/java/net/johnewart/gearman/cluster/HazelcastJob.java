package net.johnewart.gearman.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import net.johnewart.gearman.common.Job;

import java.io.IOException;

public class HazelcastJob extends Job implements DataSerializable {

    public HazelcastJob() {
    }

    public HazelcastJob(Job job) {
        cloneOtherJob(job);
    }

    public Job toJob() {
        return new Job(this);
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
        Job job = mapper.readValue(input.readUTF(), this.getClass());
        this.cloneOtherJob(job);
    }
}

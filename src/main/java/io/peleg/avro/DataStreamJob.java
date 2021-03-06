package io.peleg.avro;

import io.peleg.JobRunner;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().enableForceAvro();

        JobRunner<User> jobRunner = new JobRunner<User>(User.class, new TypeHint<List<User>>() {}, new RandomUserSourceFunction());

        jobRunner.run(env);
    }
}

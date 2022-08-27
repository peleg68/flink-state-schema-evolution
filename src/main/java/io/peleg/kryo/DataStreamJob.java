package io.peleg.kryo;

import io.peleg.Buffer;
import io.peleg.JobRunner;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        JobRunner<User> jobRunner = new JobRunner<User>(
                User.class,
                new TypeHint<List<User>>() {},
                new RandomUserSourceFunction(),
                new Buffer<User>() {});

        jobRunner.run(env);
    }
}

package io.peleg;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().enableForceAvro();

        JobRunner jobRunner = new JobRunner(
                new RandomUserSourceFunction(),
                new Buffer());

        jobRunner.run(env);
    }
}

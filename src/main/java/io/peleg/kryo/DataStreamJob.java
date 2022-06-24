package io.peleg.kryo;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(12000L, CheckpointingMode.EXACTLY_ONCE);

        env.addSource(new RandomUserSourceFunction())
                .keyBy(user -> 0)
                .window(SlidingProcessingTimeWindows.of(
                        Time.seconds(3L),
                        Time.seconds(1L)
                ))
                .aggregate(new Buffer())
                .uid("buffer")
                .print()
                .uid("sink-print");

        env.execute("flink-state-schema-evolution");
    }

    public static class Buffer implements AggregateFunction<User, List<User>, List<User>> {
        @Override
        public List<User> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<User> add(User user, List<User> users) {
            users.add(user);

            return users;
        }

        @Override
        public List<User> getResult(List<User> users) {
            return users;
        }

        @Override
        public List<User> merge(List<User> users, List<User> acc1) {
            acc1.addAll(users);

            return acc1;
        }
    }
}

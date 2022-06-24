package io.peleg.kryo;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

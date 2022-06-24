package io.peleg.kryo;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new RandomUserSourceFunction())
                .keyBy(user -> 0)
                .process(new Buffer())
                .uid("buffer")
                .print()
                .uid("sink-print");

        env.execute("flink-state-schema-evolution");
    }

    public static class Buffer extends KeyedProcessFunction<Integer, User, List<User>> {
        private ListState<User> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.state = this.getRuntimeContext().getListState(new ListStateDescriptor<User>("users", User.class) {
            });
        }

        @Override
        public void processElement(User user, KeyedProcessFunction<Integer, User, List<User>>.Context context, Collector<List<User>> collector) throws Exception {
            state.add(user);

            List<User> stateList = Lists.newArrayList(state.get().iterator());

            collector.collect(stateList);
        }
    }
}

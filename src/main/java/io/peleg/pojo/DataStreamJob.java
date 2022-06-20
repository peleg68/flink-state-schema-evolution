package io.peleg.pojo;

import io.peleg.pojo.User;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        User user0 = User.builder()
                .name("Shmulik")
                .favoriteNumber(6)
                .favoriteColor("Blue")
                .startTime(16689869L)
                .build();

        User user1 = User.builder()
                .name("David")
                .favoriteNumber(8)
                .favoriteColor("Red")
                .startTime(153252365L)
                .build();

        User user2 = User.builder()
                .name("Greg")
                .favoriteNumber(4)
                .favoriteColor("Green")
                .startTime(1543156426L)
                .build();

        env.fromElements(user0, user1, user2)
                .keyBy(user -> 0)
                .process(new Buffer())
                .print();

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

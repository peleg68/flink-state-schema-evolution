package io.peleg;

import io.peleg.avro.User;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;

public class JobRunner {
    private final SourceFunction<User> sourceFunction;
    private final Buffer aggregateFunction;

    public JobRunner(SourceFunction<User> sourceFunction, Buffer aggregateFunction) {
        this.sourceFunction = sourceFunction;
        this.aggregateFunction = aggregateFunction;
    }

    public void run(StreamExecutionEnvironment env) throws Exception {
        env.enableCheckpointing(12000L, CheckpointingMode.EXACTLY_ONCE);

        ListTypeInfo<User> userListTypeInfo = new ListTypeInfo<User>(TypeInformation.of(User.class));

        env.addSource(sourceFunction).returns(User.class)
                .keyBy(user -> 0)
                .window(SlidingProcessingTimeWindows.of(
                        Time.seconds(3L),
                        Time.seconds(1L)
                ))
                .aggregate(aggregateFunction, userListTypeInfo, userListTypeInfo)
                .uid("buffer")
                .print()
                .uid("sink-print");

        env.execute("flink-state-schema-evolution");
    }
}

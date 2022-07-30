package io.peleg;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class JobRunner<T> {
    private final Class<T> clazz;
    private final SourceFunction<T> sourceFunction;

    public JobRunner(Class<T> clazz, SourceFunction<T> sourceFunction) {
        this.clazz = clazz;
        this.sourceFunction = sourceFunction;
    }

    public void run(StreamExecutionEnvironment env) throws Exception {
        env.enableCheckpointing(12000L, CheckpointingMode.EXACTLY_ONCE);

        env.addSource(sourceFunction).returns(clazz)
                .keyBy(user -> 0)
                .window(SlidingProcessingTimeWindows.of(
                        Time.seconds(3L),
                        Time.seconds(1L)
                ))
                .aggregate(new Buffer<T>())
                .uid("buffer")
                .print()
                .uid("sink-print");

        env.execute("flink-state-schema-evolution");
    }
}

package io.peleg;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;

public class JobRunner<T> {
    private final Class<T> clazz;
    private final TypeHint<List<T>> windowOutput;
    private final SourceFunction<T> sourceFunction;

    public JobRunner(Class<T> clazz, TypeHint<List<T>> windowOutput, SourceFunction<T> sourceFunction) {
        this.clazz = clazz;
        this.windowOutput = windowOutput;
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
                .aggregate(new Buffer<T>()).returns(windowOutput)
                .uid("buffer")
                .print()
                .uid("sink-print");

        env.execute("flink-state-schema-evolution");
    }
}

package io.peleg;

import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

public class Buffer<T> implements AggregateFunction<T, List<T>, List<T>> {
    @Override
    public List<T> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<T> add(T user, List<T> users) {
        users.add(user);

        return users;
    }

    @Override
    public List<T> getResult(List<T> users) {
        return users;
    }

    @Override
    public List<T> merge(List<T> users, List<T> acc1) {
        acc1.addAll(users);

        return acc1;
    }
}

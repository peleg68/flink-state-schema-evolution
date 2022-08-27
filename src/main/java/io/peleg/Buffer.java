package io.peleg;

import io.peleg.avro.User;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

public class Buffer implements AggregateFunction<User, List<User>, List<User>> {
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

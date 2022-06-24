package io.peleg.pojo;

import io.peleg.Color;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class RandomUserSourceFunction implements SourceFunction<User> {
    private AtomicBoolean shouldKeepRunning;
    private final Random random;

    public RandomUserSourceFunction() {
        this.shouldKeepRunning = new AtomicBoolean(true);
        this.random = new Random();
    }

    @Override
    public void run(SourceContext<User> sourceContext) throws InterruptedException {
        while (shouldKeepRunning.get()) {
            User user = randomUser();

            sourceContext.collectWithTimestamp(
                    user,
                    user.getStartTime()
            );

            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        shouldKeepRunning.set(false);
    }

    private User randomUser() {
        return User.builder()
                .name(randomString())
                .favoriteNumber(random.nextInt())
                .favoriteColor(randomColor())
                .startTime(Instant.now().toEpochMilli())
                .build();
    }

    private String randomString() {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 10;

        return random.ints(leftLimit, rightLimit + 1)
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    private String randomColor() {
        return Color.values()[random.nextInt(5)]
                .toString();
    }
}

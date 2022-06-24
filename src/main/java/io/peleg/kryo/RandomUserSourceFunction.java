package io.peleg.kryo;

import io.peleg.Color;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Instant;
import java.util.Random;

public class RandomUserSourceFunction implements SourceFunction<User> {
    private boolean shouldKeepRunning;
    private final Random random;

    public RandomUserSourceFunction() {
        this.shouldKeepRunning = true;
        this.random = new Random();
    }

    @Override
    public void run(SourceContext<User> sourceContext) throws InterruptedException {
        while (shouldKeepRunning) {
            User user = randomUser();

            sourceContext.collectWithTimestamp(
                    user,
                    user.getStartTime().toEpochMilli()
            );

            Thread.sleep(3000L);
        }
    }

    @Override
    public void cancel() {
        shouldKeepRunning = false;
    }

    private User randomUser() {
        return User.builder()
                .name(randomString())
                .favoriteNumber(random.nextInt())
                .favoriteColor(randomColor())
                .startTime(Instant.now())
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

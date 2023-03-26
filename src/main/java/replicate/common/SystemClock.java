package replicate.common;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class SystemClock {
    public long nanoTime() {
        return System.nanoTime();
    }
    public long now() {
        //not guaranteed to be monotonic..
        return System.currentTimeMillis() + clockSkew.toMillis();
    }

    Duration clockSkew = Duration.of(0, ChronoUnit.MILLIS);
    public void addClockSkew(Duration clockSkew) {
        this.clockSkew = clockSkew;
    }
}

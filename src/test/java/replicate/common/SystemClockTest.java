package replicate.common;

import org.junit.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static org.junit.Assert.*;

public class SystemClockTest {

    @Test
    public void clockSkewIsAddedToSystemTime() {
        SystemClock clock = new SystemClock();
        clock.addClockSkew(Duration.of(100, ChronoUnit.MINUTES));
        assertEquals(System.currentTimeMillis() + Duration.of(100, ChronoUnit.MINUTES).toMillis(), clock.now());
    }

    @Test
    public void clockSkewCanBeNegative() {
        SystemClock clock = new SystemClock();
        clock.addClockSkew(Duration.of(-100, ChronoUnit.MINUTES));
        assertEquals(System.currentTimeMillis() - Duration.of(100, ChronoUnit.MINUTES).toMillis(), clock.now());
    }

}
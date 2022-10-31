package replicate.heartbeat;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class HeartBeatSchedulerTest {

    @Test(expected = IllegalStateException.class)
    public void notAllowStartingAnAlreadyStartedScheduler() {
        AtomicInteger i = new AtomicInteger(0);
        HeartBeatScheduler scheduler = new HeartBeatScheduler(()->{
            i.incrementAndGet();
        }, 1000l);

        scheduler.start();
        scheduler.start();
    }

}
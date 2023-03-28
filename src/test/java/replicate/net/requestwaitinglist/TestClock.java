package replicate.net.requestwaitinglist;

import replicate.common.SystemClock;

public class TestClock extends SystemClock {
    long time;

    public TestClock(long time) {
        this.time = time;
    }

    @Override
    public long nanoTime() {
        return time;
    }

    @Override
    public long now() {
        return time;
    }
}

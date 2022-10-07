package distrib.patterns.net.requestwaitinglist;

import distrib.patterns.common.SystemClock;

public class TestClock extends SystemClock {
    long nanos;

    public TestClock(long nanos) {
        this.nanos = nanos;
    }

    @Override
    public long nanoTime() {
        return nanos;
    }
}

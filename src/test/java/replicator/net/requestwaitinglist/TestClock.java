package replicator.net.requestwaitinglist;

import replicator.common.SystemClock;

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

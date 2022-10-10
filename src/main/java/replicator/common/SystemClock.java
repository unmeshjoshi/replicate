package replicator.common;

public class SystemClock {
    public long nanoTime() {
        return System.nanoTime();
    }
    public long now() {
        //not guaranteed to be monotonic..
        return System.currentTimeMillis();
    }
}

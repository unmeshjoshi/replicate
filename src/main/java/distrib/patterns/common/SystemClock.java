package distrib.patterns.common;

public class SystemClock {
    public long nanoTime() {
        return System.nanoTime();
    }
    public long now() { return System.currentTimeMillis(); }
}

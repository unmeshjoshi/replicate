package replicator.quorum;

import replicator.common.SystemClock;

import java.util.concurrent.atomic.AtomicLong;

//From https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/service/ClientState.java#L220
public class ClientState {
    SystemClock clock;
    // The biggest timestamp that was returned by getTimestamp/assigned to a query. This is global to ensure that the
    // timestamp assigned are strictly monotonic on a node, which is likely what user expect intuitively (more likely,
    // most new user will intuitively expect timestamp to be strictly monotonic cluster-wise, but while that last part
    // is unrealistic expectation, doing it node-wise is easy).
    private final AtomicLong lastTimestampMicros = new AtomicLong(0);

    public ClientState(SystemClock clock) {
        this.clock = clock;
    }

    /**
     * This clock guarantees that updates for the same ClientState will be ordered
     * in the sequence seen, even if multiple updates happen in the same millisecond.
     */
    public long getTimestamp()
    {
        while (true)
        {
            long current = currentTimeMillis() * 1000;
            long last = lastTimestampMicros.get();
            long tstamp = last >= current ? last + 1 : current;
            if (lastTimestampMicros.compareAndSet(last, tstamp))
                return tstamp;
        }
    }

    /**
     * Semantically equivalent to {@link System#currentTimeMillis()}
     */
    public long currentTimeMillis()
    {
        return clock.now();
    }
}

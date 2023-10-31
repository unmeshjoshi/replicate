package replicate.quorum;

public class StoredValue {
    public static final StoredValue EMPTY = new StoredValue("", "", Long.MIN_VALUE, 0);
    public final String key;
    public final String value;
    public final long timestamp;
    public final int generation;

    public StoredValue(String key, String value, long timestamp, int generation) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.generation = generation;
    }

    @Override
    public String toString() {
        return "StoredValue{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", timestamp=" + timestamp +
                ", generation=" + generation +
                '}';
    }
}

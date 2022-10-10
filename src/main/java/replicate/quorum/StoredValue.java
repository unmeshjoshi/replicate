package replicate.quorum;

public class StoredValue {
    public static final StoredValue EMPTY = new StoredValue("", "", Long.MIN_VALUE, 0);
    String key;
    String value;
    long timestamp;
    int generation;

    public StoredValue(String key, String value, long timestamp, int generation) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.generation = generation;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getGeneration() {
        return generation;
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

    //for jackson
    private StoredValue() {
    }
}

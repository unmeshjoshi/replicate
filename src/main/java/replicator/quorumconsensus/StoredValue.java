package replicator.quorumconsensus;

import replicator.common.MonotonicId;

public class StoredValue {
    public static final StoredValue EMPTY = new StoredValue("", "", MonotonicId.empty());
    String key;
    String value;
    private MonotonicId version;


    public StoredValue(String key, String value, MonotonicId version) {
        this.key = key;
        this.value = value;
        this.version = version;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public MonotonicId getVersion() {
        return version;
    }

    //for jackson
    private StoredValue() {
    }
}

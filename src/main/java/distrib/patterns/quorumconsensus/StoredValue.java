package distrib.patterns.quorumconsensus;

import distrib.patterns.common.MonotonicId;

public class StoredValue {
    public static final StoredValue EMPTY = new StoredValue("", "", MonotonicId.empty(), 0);
    public Integer generation;
    String key;
    String value;
    private MonotonicId version;


    public StoredValue(String key, String value, MonotonicId version, Integer generation) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.generation = generation;
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

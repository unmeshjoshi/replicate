package distrib.patterns.quorum;

class StoredValue {
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

    //for jackson
    private StoredValue() {
    }
}

package replicate.common;

/**
 * Represents the result of a write operation in the chain
 */
public class WriteResult {
    private final String key;
    private final String value;
    private final long version;
    private final boolean success;
    private final String error;

    public WriteResult(String key, String value, long version, boolean success, String error) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.success = success;
        this.error = error;
    }

    public static WriteResult success(String key, String value, long version) {
        return new WriteResult(key, value, version, true, null);
    }

    public static WriteResult failure(String key, String value, String error) {
        return new WriteResult(key, value, -1, false, error);
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getVersion() {
        return version;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getError() {
        return error;
    }
} 
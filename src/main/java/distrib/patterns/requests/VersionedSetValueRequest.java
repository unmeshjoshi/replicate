package distrib.patterns.requests;

import distrib.patterns.common.MonotonicId;

public class VersionedSetValueRequest {
    private long clientId;
    private int requestNumber;
    private String key;
    private String value;
    private MonotonicId version;

    //for jaxon
    private VersionedSetValueRequest() {
    }

    public VersionedSetValueRequest(String key, String value, long clientId, int requestNumber, MonotonicId version) {
        this.key = key;
        this.value = value;
        this.clientId = clientId;
        this.requestNumber = requestNumber;
        this.version = version;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getClientId() {
        return clientId;
    }

    public int getRequestNumber() {
        return requestNumber;
    }

    public MonotonicId getVersion() {
        return version;
    }
}



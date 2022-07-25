package requests;

public class SetValueRequest {
    private long clientId;
    private int requestNumber;
    private String key;
    private String value;
    private String attachedLease;

    //for jaxon
    private SetValueRequest() {
    }

    public SetValueRequest(String key, String value, long clientId, int requestNumber) {
        this(key, value, "", clientId, requestNumber);
    }

    public SetValueRequest(String key, String value, String attachedLease, long clientId, int requestNumber) {
        this.key = key;
        this.value = value;
        this.attachedLease = attachedLease;
        this.clientId = clientId;
        this.requestNumber = requestNumber;
    }

    public SetValueRequest(String key, String value, String attachedLease) {
        this(key, value, attachedLease, -1, -1);
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public String getAttachedLease() {
        return attachedLease;
    }

    public long getClientId() {
        return clientId;
    }

    public int getRequestNumber() {
        return requestNumber;
    }
}



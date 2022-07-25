package net.requestbatch;

//<codeFragment name="requestEntry">
class RequestEntry {
    byte[] serializedRequest;
    long createdTime;

    public RequestEntry(byte[] serializedRequest, long createdTime) {
        this.serializedRequest = serializedRequest;
        this.createdTime = createdTime;
    }

    //</codeFragment>
    public byte[] getRequest() {
        return serializedRequest;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    int size() {
        return getRequest().length;
    }
}

package net.nioserver.kafkastyle;

import common.RequestOrResponse;

import java.nio.channels.SelectionKey;

public class RequestWrapper {
    private int processorId;
    private RequestOrResponse request;
    SelectionKey key;
    public RequestWrapper(int processorId, RequestOrResponse request, SelectionKey key) {
        this.processorId = processorId;
        this.request = request;
        this.key = key;
    }

    public int getProcessorId() {
        return processorId;
    }

    public RequestOrResponse getRequest() {
        return request;
    }

    public SelectionKey getKey() {
        return key;
    }
}

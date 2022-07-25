package net.requestbatch;

import common.RequestOrResponse;
import common.JsonSerDes;
import common.RequestOrResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RequestBatch {
    List<byte[]> requests = new ArrayList<>();
    private int maxSize;
    public RequestBatch(int maxSize) {
        this.maxSize = maxSize;
    }

    public List<RequestOrResponse> getPackedRequests() {
        return requests.stream().map(b -> JsonSerDes.deserialize(b, RequestOrResponse.class)).collect(Collectors.toList());
    }

    //<codeFragment name="hasSpaceFor">
    public boolean hasSpaceFor(byte[] requestBytes) {
        return batchSize() + requestBytes.length <= maxSize;
    }
    private int batchSize() {
        return requests.stream().map(r->r.length).reduce(0, Integer::sum);
    }
    //</codeFragment>

    public boolean add(byte[] r) {
        if (!hasSpaceFor(r)) {
            return false;
        }
        return requests.add(r);
    }

    //for jackson
    RequestBatch() {
    }

    public int numRequests() {
        return requests.size();
    }

    public Integer lastId() {
        return JsonSerDes.deserialize(requests.get(requests.size() - 1), RequestOrResponse.class).getCorrelationId();
    }

    public boolean isEmpty() {
        return requests.isEmpty();
    }
}



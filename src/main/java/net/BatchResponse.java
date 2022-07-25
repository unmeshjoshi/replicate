package net;

import common.RequestOrResponse;
import java.util.List;

public class BatchResponse {
    List<RequestOrResponse> responseList;

    public BatchResponse(List<RequestOrResponse> responseList) {
        this.responseList = responseList;
    }

    public List<RequestOrResponse> getResponseList() {
        return responseList;
    }

    //for jackson
    private BatchResponse() {
    }
}

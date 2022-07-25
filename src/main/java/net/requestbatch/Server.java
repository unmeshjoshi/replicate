package net.requestbatch;

import common.Config;
import common.JsonSerDes;
import common.Message;
import common.RequestOrResponse;
import requests.SetValueRequest;
import common.RequestId;
import net.*;
import net.nioserver.zkstyle.NIOSocketListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Server {
    private final NIOSocketListener listener;
    private Config config;
    private InetAddressAndPort listenAddress;
    Map<String, String> kv = new HashMap<>();

    //<codeFragment name="Receiver">
    public Server(Config config, InetAddressAndPort listenAddress) throws IOException {
        this.config = config;
        this.listenAddress = listenAddress;
        this.listener = new NIOSocketListener(request -> handleServerMessage(request), listenAddress);

    }
    //</codeFragment>

    public void start() {
        listener.start();
    }

    private void handleServerMessage(Message<RequestOrResponse> message) {
        try {
            RequestOrResponse request = message.getRequest();
            if (request.getRequestId() == RequestId.SetValueRequest.getId()) {
                SetValueRequest setValueRequest = JsonSerDes.deserialize(request.getMessageBodyJson(), SetValueRequest.class);
                kv.put(setValueRequest.getKey(), setValueRequest.getValue());
                message.getClientConnection().write(new RequestOrResponse(request.getGroupId(), RequestId.SetValueResponse.getId(), "Success".getBytes(), request.getCorrelationId(), listenAddress));

            } else if (request.getRequestId() == RequestId.BatchRequest.getId()) {
                handleBatchRequest(request, message.getClientConnection());
            }
        } catch (Exception e) {
            e.printStackTrace();;
        }


    }

    //<codeFragment name="handleBatchRequest">
    private void handleBatchRequest(RequestOrResponse batchRequest, ClientConnection clientConnection) {
        RequestBatch batch = JsonSerDes.deserialize(batchRequest.getMessageBodyJson(), RequestBatch.class);
        List<RequestOrResponse> requests = batch.getPackedRequests();
        List<RequestOrResponse> responses = new ArrayList<>();
        for (RequestOrResponse request : requests) {
            RequestOrResponse response = handleSetValueRequest(request);
            responses.add(response);
        }
        sendResponse(batchRequest, clientConnection, new BatchResponse(responses));
    }

    private RequestOrResponse handleSetValueRequest(RequestOrResponse request) {
        SetValueRequest setValueRequest = JsonSerDes.deserialize(request.getMessageBodyJson(), SetValueRequest.class);
        kv.put(setValueRequest.getKey(), setValueRequest.getValue());
        RequestOrResponse response = new RequestOrResponse(RequestId.SetValueResponse.getId(), "Success".getBytes(), request.getCorrelationId());
        return response;
    }
    //</codeFragment>

    private void sendResponse(RequestOrResponse request, ClientConnection clientConnection, BatchResponse batchResponse) {
        clientConnection.write(new RequestOrResponse(request.getGroupId(), RequestId.BatchResponse.getId(),
                JsonSerDes.serialize(batchResponse), request.getCorrelationId(), listenAddress));
    }
}

package net.nioserver.kafkastyle;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class RequestChannel {
    private int numProcessors;
    private int queueSize;
    private BlockingQueue<RequestWrapper> requestQueue;
    private List<BlockingQueue<ResponseWrapper>> responseQueues;
    public RequestChannel(int numProcessors, int queueSize) {
        this.numProcessors = numProcessors;
        this.queueSize = queueSize;
        this.requestQueue = new ArrayBlockingQueue<RequestWrapper>(queueSize);
        this.responseQueues = new ArrayList<BlockingQueue<ResponseWrapper>>();
        for (int i = 0; i < numProcessors; i++) {
             responseQueues.add(new LinkedBlockingDeque<ResponseWrapper>());
        }
    }

    public void sendRequest(RequestWrapper request) throws InterruptedException {
        requestQueue.put(request);
    }

    public void sendResponse(ResponseWrapper response) {
        responseQueues.get(response.getProcessorId()).add(response);
    }

    public RequestWrapper receiveRequest() throws InterruptedException {
        return requestQueue.take();
    }

    public ResponseWrapper receiveResponse(int processorId) {
        return responseQueues.get(processorId).poll();
    }

}

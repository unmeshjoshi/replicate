package distrib.patterns.generation;

//Two problems:
//1. How to order requests. They can be conurrent or happening one after the other, with nodes failing or messages
// getting lost.
// solution: Assign a unique number to each request. To maintain the uniqueness ask the quorum to check if
// they have seen a higher value. If yes, then pick up a next value and repeat. When quorum of servers
// accept that this is the highest value, then use that number.

//2. How to know of quorum of replicas is going to execute the command?
//   First send the command to all the replicas, and execute only if a quorum responds

//3. How all the replicas know that quorum of replicas have handled this command?
// Send commit message to all the replicas. Execute the command only after receiving the commit message.

//4. What if we get some lower numbered request?
//   Reject the request and do not execute it.

//5. What if the sender fails after sending requests to all the nodes.
//   What if some cluster nodes fail after accepting the request
//   What if some cluster nodes get disconnected after accepting the request
//   With quorum we can tolerate minority node failures
//   What if accepted by only a minority of nodes.
//       a accepts and then sender fails.
//    Next sender can connect to b and c
      //  b accepts and then sender fails.

// Next sender can connect to a and b.
// Which value to choose? One of the values from a or b might be the value from previous majority..

//2. How to know if quorum of replicas have accepted the request
      //Some responses might get lost and we might not know about the accepted request.
//   Send commit message to all the replicas.
//     Some replicas might not receive commit message.
//   execute it. //this makes sure all the replicas will execute this request.
// How to handle various failures which might happen?
//3. What if client fails
// a request sender can play two roles. Either send the request that is it was going to send.. or
// repair previously incomplete requests..
// A proposer can keep the state stored on the disk and keep retrying..
// But what if the proposer fails?

import com.google.common.util.concurrent.Uninterruptibles;
import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.requestwaitinglist.RequestCallback;
import distrib.patterns.paxos.PrepareResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class GenerationVoting extends Replica {
    int generation;

    public GenerationVoting(Config config, SystemClock clock, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peerAddresses) throws IOException {
        super(config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses);
    }

    @Override
    public void handleClientRequest(Message<RequestOrResponse> message) {
        RequestOrResponse request = message.getRequest();
        if (request.getRequestId() == RequestId.NextNumberRequest.getId()) {
            int proposedNumber = 1;
            while(true) {
                PrepareRequest nr = new PrepareRequest(proposedNumber);
                PrepareCallback callback = new PrepareCallback(getNoOfReplicas());
                sendRequestToReplicas(callback, RequestId.PrepareRequest, nr);
                if (callback.isQuorumPrepared()) {
                    message.getClientConnection().write(new RequestOrResponse(RequestId.PrepareRequest.getId(), JsonSerDes.serialize(generation), request.getCorrelationId()));
                    break;
                }
                proposedNumber = proposedNumber + 1;//try next number
            }
        }
    }


    class PrepareCallback implements RequestCallback<PrepareResponse> {
        int clusterSize;
        int quorum;
        CountDownLatch latch;
        List<PrepareResponse> promises = new ArrayList<>();

        public PrepareCallback(int clusterSize) {
            this.clusterSize = clusterSize;
            this.quorum =  clusterSize / 2 + 1;
            this.latch = new CountDownLatch(clusterSize);
        }

        @Override
        public void onResponse(PrepareResponse r) {
            promises.add(r);
            latch.countDown();
        }

        @Override
        public void onError(Throwable e) {
        }

        public boolean isQuorumPrepared() {
            Uninterruptibles.awaitUninterruptibly(latch);
            return promises.stream().filter(p -> p.isPromised()).count() >= quorum;
        }
    }

    static class RequestDetails<T> {
        RequestHandler handler;
        Class requestClass;

        public RequestDetails(RequestHandler<T> handler, Class<T> requestClass) {
            this.handler = handler;
            this.requestClass = requestClass;
        }
    }

    static interface RequestHandler<T> {
        public void handleRequest(T request, InetAddressAndPort fromAddress, int correlationId);
    }

    Map<RequestId, RequestDetails> requestMap = new HashMap<>();
    {
        requestMap.put(RequestId.PrepareRequest, new RequestDetails<>(this::handlePrepareRequest, PrepareRequest.class));
        requestMap.put(RequestId.Promise, new RequestDetails<>(this::handleResponse, PrepareResponse.class));
    }
    @Override
    public void handleServerMessage(Message<RequestOrResponse> message) {
        RequestOrResponse request = message.getRequest();
        RequestDetails requestDetails = requestMap.get(RequestId.valueOf(request.getRequestId()));
        Object r = JsonSerDes.deserialize(request.getMessageBodyJson(), requestDetails.requestClass);
        requestDetails.handler.handleRequest(r, request.getFromAddress(), request.getCorrelationId());
    }

    public <T> void handleResponse(PrepareResponse response, InetAddressAndPort fromAddress, int correlationId) {
        requestWaitingList.handleResponse(correlationId, response);
    }

    private void handlePrepareRequest(PrepareRequest nextNumberRequest, InetAddressAndPort fromAddress, int correlationId) {
        PrepareResponse response;
        if (generation > nextNumberRequest.getNumber()) {
            response = new PrepareResponse(false);
        } else {
            generation = nextNumberRequest.getNumber();
            response = new PrepareResponse(true);
        }
        sendOneway(fromAddress, RequestId.Promise, response, correlationId);
    }
}
package distrib.patterns.generationvoting;

//Two problems:
//1. How to order requests. They can be concurrent or happening one after the other, with nodes failing or messages
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

import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.paxos.PrepareResponse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class GenerationVoting extends Replica {
    int generation;

    public GenerationVoting(Config config, SystemClock clock, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peerAddresses) throws IOException {
        super(config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses);
        {
            requestHandler(RequestId.NextNumberRequest, this::handleNextNumberRequest, NextNumberRequest.class);
            messageHandler(RequestId.PrepareRequest, this::handlePrepareRequest, PrepareRequest.class);
            responseMessageHandler(RequestId.Promise, PrepareResponse.class);
        }
    }

    class PrepareCallback extends BlockingQuorumCallback<PrepareResponse> {
        public PrepareCallback(int totalResponses) {
            super(totalResponses);
        }
        public boolean isQuorumPrepared() {
           return blockAndGetQuorumResponses()
                   .values()
                   .stream()
                   .filter(p -> p.isPromised()).count() >= quorum;
        }
    }

    CompletableFuture<Integer> handleNextNumberRequest(NextNumberRequest request) {
        int proposedNumber = 1;
        while(true) {
            PrepareRequest nr = new PrepareRequest(proposedNumber);
            PrepareCallback callback = new PrepareCallback(getNoOfReplicas());
            sendRequestToReplicas(callback, RequestId.PrepareRequest, nr);
            if (callback.isQuorumPrepared()) {
                //TODO:Consider using blocking methods for ease of understanding.
                return CompletableFuture.completedFuture(proposedNumber);
            }
            proposedNumber = proposedNumber + 1;//try next number
        }
    }

    private PrepareResponse handlePrepareRequest(PrepareRequest nextNumberRequest) {
        PrepareResponse response;
        if (generation > nextNumberRequest.getNumber()) {
            response = new PrepareResponse(false);
        } else {
            generation = nextNumberRequest.getNumber();
            response = new PrepareResponse(true);
        }
        return response;
    }
}
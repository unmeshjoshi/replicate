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
import distrib.patterns.generationvoting.messages.NextNumberRequest;
import distrib.patterns.generationvoting.messages.PrepareRequest;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.paxos.WriteTimeoutException;
import distrib.patterns.paxos.messages.PrepareResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class GenerationVoting extends Replica {
    //epoch/term/generation
    int generation = 0;
    private Logger logger = LogManager.getLogger(GenerationVoting.class);

    public GenerationVoting(String name, Config config, SystemClock clock, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peerAddresses) throws IOException {
        super(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses);

    }

    @Override
    protected void registerHandlers() {
        handlesRequestAsync(RequestId.NextNumberRequest, this::handleNextNumberRequest, NextNumberRequest.class);
        handlesMessage(RequestId.Prepare, this::handlePrepareRequest, PrepareRequest.class)
                .respondsWithMessage(RequestId.Promise, PrepareResponse.class);
    }

    CompletableFuture<Integer> handleNextNumberRequest(NextNumberRequest request) {
        int proposedNumber = 1;
        return proposeNumber(proposedNumber, 1);
    }

    private CompletableFuture<Integer> proposeNumber(int proposedNumber, int attempt) {
        PrepareRequest nr = new PrepareRequest(proposedNumber);
        var callback = new AsyncQuorumCallback<PrepareResponse>(getNoOfReplicas(), p -> p.promised);

        sendMessageToReplicas(callback, RequestId.Prepare, nr);

        return callback.getQuorumFuture().handle((r, e) -> {
            if (e == null) {
                return CompletableFuture.completedFuture(proposedNumber);
            }

            return proposeNumber(attempt + 1, proposedNumber + 1);
        }).thenCompose(x -> x);
    }

    private PrepareResponse handlePrepareRequest(PrepareRequest nextNumberRequest) {
        if (nextNumberRequest.getNumber() > generation) { //accept only if 'strictly greater'
            generation = nextNumberRequest.getNumber();
            logger.info("accepting " + generation + " in " + getName());
            return new PrepareResponse(true);
        }
        logger.info("rejecting " + generation + " in " + getName());
        return new PrepareResponse(false);
    }
}
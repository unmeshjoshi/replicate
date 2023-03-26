package replicate.generationvoting;



//Assign a unique number to each request. To maintain the uniqueness ask the quorum to check if
// they have seen a higher value. If yes, then pick up a next value and repeat. When quorum of servers
// accept that this is the highest value, then use that number.
// The node checks for strict > instead of >=. (In paxos literature its >=, which then requires
//additional measures to guarantee uniqueness such as use of serverId.
//@see MonotonicId class.
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replicate.common.*;
import replicate.generationvoting.messages.NextNumberRequest;
import replicate.generationvoting.messages.PrepareRequest;
import replicate.net.InetAddressAndPort;
import replicate.paxos.messages.PrepareResponse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

//the ballot/number needs to be persisted on each replica
//the execution needs to be thread safe.. done using SingularUpdateQueue
public class BallotVoting extends Replica {
    //epoch/term/generation
    //this is durable.
    int ballot = 0;

    private Logger logger = LogManager.getLogger(BallotVoting.class);
    private int maxAttmpts = 4;

    public BallotVoting(String name, Config config, SystemClock clock, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peerAddresses) throws IOException {
        super(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses);

    }

    @Override
    protected void registerHandlers() {
        handlesRequestAsync(MessageId.NextNumberRequest, this::handlePrepare, NextNumberRequest.class);
        handlesMessage(MessageId.Prepare, this::handlePrepareRequest, PrepareRequest.class);
        handlesMessage(MessageId.Promise, this::handlePrepareResponse, PrepareResponse.class);
    }

    private void handlePrepareResponse(Message<PrepareResponse> prepareResponseMessage) {
        handleResponse(prepareResponseMessage);
    }

    CompletableFuture<Integer> handlePrepare(NextNumberRequest request) {
        int proposedNumber = 0;
        return proposeNumber(proposedNumber);
    }

    private CompletableFuture<Integer> proposeNumber(int proposedNumber) {
        int maxAttempts = 5;
        AtomicInteger proposal = new AtomicInteger(proposedNumber);
        return FutureUtils.retryWithRandomDelay(() -> {
            var resultFuture = new CompletableFuture<Integer>();
            PrepareRequest nr = new PrepareRequest(proposal.incrementAndGet());
            var callback = new AsyncQuorumCallback<PrepareResponse>(getNoOfReplicas(), p -> p.promised);
            sendMessageToReplicas(callback, MessageId.Prepare, nr);
            callback.getQuorumFuture().whenComplete((result, exception) -> {
                if (exception != null) {
                    resultFuture.completeExceptionally(exception);
                } else {
                    resultFuture.complete(proposal.intValue());
                }
            });
            return resultFuture;
        }, maxAttempts, singularUpdateQueueExecutor);

    }

    private void handlePrepareRequest(Message<PrepareRequest> message) {
        //no synchronized in here..
        var  prepareRequest = message.messagePayload();
        boolean promised = false;
        if (prepareRequest.getProposedBallot() > ballot) { //accept only if 'strictly greater'
            ballot = prepareRequest.getProposedBallot();
            logger.info(getName() + " accepting " + ballot + " in " + getName());
            promised = true;
        }
        logger.info(getName() + " rejecting " + ballot + " in " + getName());
        sendOneway(message.getFromAddress(), new PrepareResponse(promised), message.getCorrelationId());
    }
}
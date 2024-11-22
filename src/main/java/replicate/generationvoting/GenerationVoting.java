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
import replicate.wal.DurableKVStore;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

//the ballot/number needs to be persisted on each replica
//the execution needs to be thread safe.. done using SingularUpdateQueue
public class GenerationVoting extends Replica {
    //epoch/term/generation
    //this is durable.
    //DurableKVStore
    int generation = 0; //this needs to be durable.

    DurableKVStore ballotStore;

    private Logger logger = LogManager.getLogger(GenerationVoting.class);
    private int maxAttmpts = 4;

    public GenerationVoting(String name, Config config, SystemClock clock, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peerAddresses) throws IOException {
        super(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses);
        ballotStore = new DurableKVStore(config);
    }

    @Override
    protected void registerHandlers() {
        handlesRequestAsync(MessageId.NextNumberRequest, this::handleNextNumberRequest, NextNumberRequest.class);
        handlesMessage(MessageId.Prepare, this::handlePrepareRequest, PrepareRequest.class);
        handlesMessage(MessageId.Promise, this::handlePrepareResponse, PrepareResponse.class);
    }

    private void handlePrepareResponse(Message<PrepareResponse> prepareResponseMessage) {
        handleResponse(prepareResponseMessage);
    }

    CompletableFuture<Integer> handleNextNumberRequest(NextNumberRequest request) {
       return proposeNumber(generation);
    }

    private CompletableFuture<Integer> proposeNumber(int proposedNumber) {
        int maxAttempts =   5;
        AtomicInteger proposal = new AtomicInteger(proposedNumber);
        return FutureUtils.retryWithRandomDelay(() -> {
            PrepareRequest nr = new PrepareRequest(proposal.incrementAndGet());
            var callback = new AsyncQuorumCallback<PrepareResponse>(getNoOfReplicas(), p -> p.promised);

            sendMessageToReplicas(callback, MessageId.Prepare, nr);

            return callback.getQuorumFuture()
                    .thenApply(result -> proposal.intValue())
                    .exceptionally(ex -> {
                        throw new RuntimeException("Exception occurred while processing the request.", ex);
                    });
        }, maxAttempts, singularUpdateQueueExecutor);
    }

    private void handlePrepareRequest(Message<PrepareRequest> message) {
        //no synchronized in here..
        var  prepareRequest = message.messagePayload();
        boolean promised = false;
        if (prepareRequest.proposedGeneration > generation) { //accept only if 'strictly
            // greater'
            generation = prepareRequest.proposedGeneration;
            logger.info(getName() + " accepting " + generation + " in " + getName());
            promised = true;
        }
        logger.info(getName() + " rejecting " + generation + " in " + getName());
        sendOneway(message.getFromAddress(), new PrepareResponse(promised), message.getCorrelationId());
    }
}
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class BallotVoting extends Replica {
    //epoch/term/generation
    int ballot = 0;
    private Logger logger = LogManager.getLogger(BallotVoting.class);
    private int maxAttmpts = 4;

    public BallotVoting(String name, Config config, SystemClock clock, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peerAddresses) throws IOException {
        super(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses);

    }

    @Override
    protected void registerHandlers() {
        handlesRequestAsync(RequestId.NextNumberRequest, this::handleNextNumberRequest, NextNumberRequest.class);
        handlesMessage(RequestId.Prepare, this::handlePrepareRequest, PrepareRequest.class)
                .respondsWithMessage(RequestId.Promise, PrepareResponse.class);
    }

    CompletableFuture<Integer> handleNextNumberRequest(NextNumberRequest request) {
        int proposedNumber = 0;
        return proposeNumber(proposedNumber);
    }

    ScheduledExecutorService retryExecutor = Executors.newSingleThreadScheduledExecutor();
    private CompletableFuture<Integer> proposeNumber(int proposedNumber) {
        int maxAttempts = 5;
        AtomicInteger proposal = new AtomicInteger(proposedNumber);
        return FutureUtils.retryWithRandomDelay(() -> {
            var resultFuture = new CompletableFuture<Integer>();
            PrepareRequest nr = new PrepareRequest(proposal.incrementAndGet());
            var callback = new AsyncQuorumCallback<PrepareResponse>(getNoOfReplicas(), p -> p.promised);
            sendMessageToReplicas(callback, RequestId.Prepare, nr);
            callback.getQuorumFuture().whenComplete((result, exception) -> {
                if (exception != null) {
                    resultFuture.completeExceptionally(exception);
                } else {
                    resultFuture.complete(proposal.intValue());
                }
            });
            return resultFuture;
        }, maxAttempts, retryExecutor);

    }

    private PrepareResponse handlePrepareRequest(PrepareRequest nextNumberRequest) {
        if (nextNumberRequest.getNumber() > ballot) { //accept only if 'strictly greater'
            ballot = nextNumberRequest.getNumber();
            logger.info(getName() + " accepting " + ballot + " in " + getName());
            return new PrepareResponse(true);
        }
        logger.info(getName() + " rejecting " + ballot + " in " + getName());
        return new PrepareResponse(false);
    }
}
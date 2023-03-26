package replicate.twophaseexecution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replicate.common.*;
import replicate.net.InetAddressAndPort;
import replicate.twophaseexecution.messages.*;
import replicate.vsr.CompletionCallback;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Adds a phase to know about incomplete commits.
 * +------+              +-------+          +--------+        +--------+
 * |client|              | node1 |          | node2  |        | node3  |
 * |      |              |       |          |        |        |        |
 * +--+---+              +---+---+          +----+---+        +----+---+
 *    |     command          |                   |                 |
 *    +--------------------->+                   |                 |
 *    |                      +----+ (know about  |                 |
 *    |                      |    |  incomplete  |                 |
 *    |                      <----+  completes)  |                 |
 *    |                      |       prepare     |                 |
 *    |                      +<----------------->+                 |
 *    |                      |                   |                 |
 *    |                      +<----------------------------------->+
 *    |                      +---+               |                 |
 *    |                      |   |   propose     |                 |
 *    |                      +^---+              |                 |
 *    |                      |------------------^+                 |
 *    |                      +<------------------+                 |
 *    |                      |       accepted    |                 |
 *    |                      +------------------------------------->
 *    |                      |<------------------------------------+
 *    |                      +                   |                 |
 *    |                      |                   |                 |
 *    |                      +---+               |                 |
 *    |                      |   |               |                 |
 *    |                      +<--+               |                 |
 *    |                      |       commit      |                 |
 *    |                      +------------------->                 |
 *    |                      <-------------------|                 +
 *    |                      |-------------------------------------|
 *    |                      +<------------------------------------+
 *    |    result           ++
 *    ^---------------------+
 */
public class NonBlockingTwoPhaseExecution extends TwoPhaseExecution {
    private static Logger logger = LogManager.getLogger(NonBlockingTwoPhaseExecution.class);

    public NonBlockingTwoPhaseExecution(String name, Config config, SystemClock clock, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peerAddresses) throws IOException {
        super(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses);
    }

    @Override
    protected void registerHandlers() {
        super.registerHandlers();
        handlesMessage(MessageId.Prepare, this::handlePrepare, PrepareRequest.class);
        handlesMessage(MessageId.Promise, this::handlePromise, PrepareResponse.class);
    }

    private void handlePromise(Message<PrepareResponse> prepareResponseMessage) {
        handleResponse(prepareResponseMessage);
    }

    private void handlePrepare(Message<PrepareRequest> message) {
        var t = message.messagePayload();
        byte[] b = acceptedCommand == null? null:acceptedCommand.serialize(); //TODO: Use Optional
        sendOneway(message.getFromAddress(), new PrepareResponse(b), message.getCorrelationId());
    }

    @Override
    CompletableFuture<ExecuteCommandResponse> handleExecute(ExecuteCommandRequest newCommand) {
        CompletionCallback<ExecuteCommandResponse> callback = new CompletionCallback();
        requestWaitingList.add(requestIdentifier(newCommand.command), callback);
        //phase 1
        AsyncQuorumCallback<PrepareResponse> prepareCallback = new AsyncQuorumCallback<>(getNoOfReplicas());
        PrepareRequest prepare = new PrepareRequest();
        sendMessageToReplicas(prepareCallback, prepare.getMessageId(), prepare);
        CompletableFuture<Map<InetAddressAndPort, PrepareResponse>> quorumFuture = prepareCallback.getQuorumFuture();
        quorumFuture.thenCompose(r -> {
            //phase 2
            byte[] command = pickCommandToExecute(r.values().stream().toList(), newCommand.command);
            ProposeRequest proposal = new ProposeRequest(command);
            AsyncQuorumCallback<ProposeResponse> proposeQuorumCallback = new AsyncQuorumCallback<>(getNoOfReplicas(), p -> p.isAccepted());
            sendMessageToReplicas(proposeQuorumCallback, proposal.getMessageId(), proposal);
            return proposeQuorumCallback.getQuorumFuture().thenCompose(a -> {
                //phase 3
                AsyncQuorumCallback<CommitCommandResponse> commitQuorumCalback = new AsyncQuorumCallback<>(getNoOfReplicas(), c -> c.isCommitted());
                CommitCommandRequest commitCommandRequest = new CommitCommandRequest(command);
                sendMessageToReplicas(commitQuorumCalback, commitCommandRequest.getMessageId(), commitCommandRequest);
                return commitQuorumCalback.getQuorumFuture();
            });
        });
        return callback.getFuture();
    }

    private byte[] pickCommandToExecute(List<PrepareResponse> prepareResponses, byte[] newCommand) {
        List<PrepareResponse> previouslyAcceptedCommands = prepareResponses.stream().filter(p -> p.command != null).collect(Collectors.toList());
        //TODO: If there are multiple commands, which command to pick?
        //       We need a way to order the requests.
        //       Go to Paxos.
        logger.info("Picking up previously accepted command " + previouslyAcceptedCommands);
        return previouslyAcceptedCommands.isEmpty() ? newCommand:previouslyAcceptedCommands.get(0).command;
    }
}

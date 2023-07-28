package replicate.twophaseexecution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replicate.common.*;
import replicate.net.InetAddressAndPort;
import replicate.twophaseexecution.messages.*;
import replicate.vsr.CompletionCallback;
import replicate.wal.Command;
import replicate.wal.DurableKVStore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * For every node to know if all other nodes have accepted a command, needs
 * execution in two phases.
 * Phase1: Tell every node to accept a command to execute
 * Nodes return a response telling that they have acccepted a command.
 * Phase2: Once majority of the nodes accept the command. Tell everyone to commit it.
 * This way everyone knows that all other nodes will also be executing the command.
 * <p>
 * +-------+              +--------+          +--------+       +--------+
 * |       |              |        |          |        |       |        |
 * |Client |              | node1  |          | node2  |       | node3  |
 * |       |              |        |          |        |       |        |
 * +---+---+              +---+----+          +---+----+       +----+---+
 * |                      |                   |                 |
 * |     command          |                   |                 |
 * +--------------------->+                   |                 |
 * |                      +---+               |                 |
 * |                      |   |               |                 |
 * |                      +<--                |                 |
 * |                      |-------propose---->|                 |
 * |                      +<------accepted--- |                 |
 * |                      |                   |                 |
 * |                      +--------------propose--------------->|
 * |                      |<-------------accepted---------------|
 * |                      +                   |                 |
 * |                      |
 * |                   |
 * |                      +---+               |                 |
 * |                      |   |               |                 |
 * |                      +<--+               |                 |
 * |                      | commit/execute    |                 |
 * |                      +------------------->                 |
 * |                      <-------------------|                 +
 * |                      |------------------------------------>|
 * |                      +<------------------------------------+
 * |                     ++
 * |<--------------------+
 */

public class DeferredCommitment extends Replica {
    Command acceptedCommand; //intermediate storage waiting for confirmation.
    DurableKVStore kvStore; //final storage exposed to clients.

    // what to do with other requests?
    //if not accepting other requests..
    //how to repair nodes which missed commit requests..
    // What if commit requests are lost?



    private static Logger logger = LogManager.getLogger(DeferredCommitment.class);

    public DeferredCommitment(String name, Config config, SystemClock clock, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peerAddresses) throws IOException {
        super(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses);
        this.kvStore = new DurableKVStore(config);
    }

    @Override
    protected void registerHandlers() {
        handlesMessage(MessageId.ProposeRequest, this::handlePropose, ProposeRequest.class);
        handlesMessage(MessageId.ProposeResponse, this::handleProposeResponse, ProposeResponse.class);
        handlesMessage(MessageId.Commit, this::handleCommit, CommitCommandRequest.class);
        handlesMessage(MessageId.CommitResponse, this::handleCommitReponse, CommitCommandResponse.class);

        handlesRequestAsync(MessageId.ExcuteCommandRequest, this::handleExecute, ExecuteCommandRequest.class);
    }

    private void handleCommitReponse(Message<CommitCommandResponse> commitCommandResponseMessage) {
        handleResponse(commitCommandResponseMessage);
    }

    private void handleProposeResponse(Message<ProposeResponse> proposeResponseMessage) {
        handleResponse(proposeResponseMessage);
    }

    //atomic operation... singularupdatequeue or locks.
    private void handleCommit(Message<CommitCommandRequest> message) {
        CommitCommandRequest t = message.messagePayload();
        Command command = getCommand(t.getCommand());
        acceptedCommand = command;
        if (command instanceof CompareAndSwap) {
            CompareAndSwap cas = (CompareAndSwap) command;
            Optional<String> existingValue = Optional.ofNullable(kvStore.get(cas.getKey()));
            boolean isCommitted = false;
            if (existingValue.equals(cas.getExistingValue())) {
                kvStore.put(cas.getKey(), cas.getNewValue());
                isCommitted = true;
            }
            sendOneway(message.getFromAddress(), new CommitCommandResponse(isCommitted, existingValue), message.getCorrelationId());
            //complete pending client requests.
            logger.info("Completing client request for " + requestIdentifier(t.getCommand()));
            requestWaitingList.handleResponse(requestIdentifier(command.serialize()), new ExecuteCommandResponse(existingValue, isCommitted));
            return;
        }
        throw new IllegalArgumentException("Unknown command " + command);
    }

    static Command getCommand(byte[] command) {
        return Command.deserialize(new ByteArrayInputStream(command));
    }

    CompletableFuture<ExecuteCommandResponse> handleExecute(ExecuteCommandRequest t) {
        byte[] command = getCommand(t.command).serialize();
        CompletionCallback<ExecuteCommandResponse> completionCallback = new CompletionCallback();
        requestWaitingList.add(requestIdentifier(command), completionCallback);
        executeTwoPhases(command);
        return completionCallback.getFuture();
    }

    private void executeTwoPhases(byte[] command) {
        //phase 1 - propose
        ProposeRequest proposal = new ProposeRequest(command);
        AsyncQuorumCallback<ProposeResponse> proposeQuorumCallback = new AsyncQuorumCallback<>(getNoOfReplicas(), p -> p.isAccepted());
        sendMessageToReplicas(proposeQuorumCallback, proposal.getMessageId(), proposal);

        //phase 2 - commit
        proposeQuorumCallback.getQuorumFuture().thenCompose(a -> {
            AsyncQuorumCallback<CommitCommandResponse> commitQuorumCallback = new AsyncQuorumCallback<>(getNoOfReplicas(), c -> c.isCommitted());
            CommitCommandRequest commitCommandRequest = new CommitCommandRequest(command);
            sendMessageToReplicas(commitQuorumCallback, commitCommandRequest.getMessageId(), commitCommandRequest);
            return commitQuorumCallback.getQuorumFuture();
        });
    }

    protected static BigInteger requestIdentifier(byte[] bytes) {
        return Utils.hash(bytes);
    }

    private void handlePropose(Message<ProposeRequest> message) {
        var proposeRequest = message.messagePayload();
        //dont accept lower numbered commands.
        acceptedCommand = getCommand(proposeRequest.getCommand());
        sendOneway(message.getFromAddress(), new ProposeResponse(true), message.getCorrelationId());
    }

    public String getValue(String key) {
        return kvStore.get(key);
    }
}

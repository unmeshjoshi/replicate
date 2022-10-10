package replicate.twophaseexecution;

import replicate.common.Config;
import replicate.common.RequestId;
import replicate.common.SystemClock;
import replicate.net.InetAddressAndPort;
import replicate.twophaseexecution.messages.*;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Adds a phase to know about incomplete commits.
 * +------+              +-------+          +--------+        +--------+
 * |client|              | node1 |          | node2  |        | node3  |
 * |      |              |       |          |        |        |        |
 * +--+---+              +---+---+          +----+---+        +----+---+
 *    |     command          |                   |                 |
 *    +--------------------->+                   |                 |
 *    |                      +----+              |                 |
 *    |                      |    |   GetIncompleteCommits         |
 *    |                      <----+              |                 |
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
    public NonBlockingTwoPhaseExecution(String name, Config config, SystemClock clock, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peerAddresses) throws IOException {
        super(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses);
    }

    @Override
    protected void registerHandlers() {
        super.registerHandlers();
        handlesRequestBlocking(RequestId.Prepare, this::handlePrepare, PrepareRequest.class)
                .respondsWith(RequestId.Promise, PrepareResponse.class);
    }

    private PrepareResponse handlePrepare(PrepareRequest t) {
        byte[] b = acceptedCommand == null? null:acceptedCommand.serialize(); //TODO: Use Optional
        return new PrepareResponse(b);
    }

    ExecuteCommandResponse handleExecute(ExecuteCommandRequest t) {
        PrepareRequest prepare = new PrepareRequest();
        List<PrepareResponse> prepareResponses = blockingSendToReplicas(prepare.getRequestId(), prepare);
        byte[] command = pickCommandToExecute(prepareResponses, t.command);
        ProposeRequest proposal = new ProposeRequest(command);
        List<ProposeResponse> proposalResponses = blockingSendToReplicas(proposal.getRequestId(), proposal);
        if (proposalResponses.stream().filter(r -> r.isAccepted()).count() >= quorum()) {
            CommitCommandResponse c = sendCommitRequest(new CommitCommandRequest(command));
            return new ExecuteCommandResponse(c.getResponse(), c.isCommitted());
        };
        return ExecuteCommandResponse.notCommitted();
    }

    private byte[] pickCommandToExecute(List<PrepareResponse> prepareResponses, byte[] command) {
        List<PrepareResponse> previouslyAcceptedCommands = prepareResponses.stream().filter(p -> p.command != null).collect(Collectors.toList());
        //TODO: If there are multiple commands, which command to pick?
        //       We need a way to order the requests.
        //       Go to Paxos.
        return previouslyAcceptedCommands.isEmpty() ? command:previouslyAcceptedCommands.get(0).command;
    }
}

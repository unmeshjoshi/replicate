package distrib.patterns.twophasecommit;

import distrib.patterns.common.Config;
import distrib.patterns.common.Replica;
import distrib.patterns.common.RequestId;
import distrib.patterns.common.SystemClock;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.twophasecommit.messages.*;
import distrib.patterns.wal.Command;
import distrib.patterns.wal.DurableKVStore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class TwoPhaseCommit extends Replica {
    Command acceptedCommand;
    DurableKVStore kvStore;
    public TwoPhaseCommit(String name, Config config, SystemClock clock, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peerAddresses) throws IOException {
        super(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses);
        this.kvStore = new DurableKVStore(config);
    }

    @Override
    protected void registerHandlers() {
        handlesRequest(RequestId.ProposeRequest, this::handlePropose, ProposeRequest.class)
                .respondsWith(RequestId.ProposeResponse, ProposeResponse.class);
        handlesRequest(RequestId.Commit, this::handleCommit, CommitCommandRequest.class)
                .respondsWith(RequestId.CommitResponse, CommitCommandResponse.class);
        handlesRequest(RequestId.ExcuteCommandRequest, this::handleExecute, ExecuteCommandRequest.class)
                .respondsWith(RequestId.ExcuteCommandResponse, ExecuteCommandResponse.class);
    }

    private CommitCommandResponse handleCommit(CommitCommandRequest t) {
        Command command = getCommand(t.getCommand());
        acceptedCommand = command;
        if (command instanceof CompareAndSwap) {
            CompareAndSwap cas = (CompareAndSwap)command;
            Optional<String> existingValue = Optional.ofNullable(kvStore.get(cas.getKey()));
            if (existingValue.equals(cas.getExistingValue())) {
                kvStore.put(cas.getKey(), cas.getNewValue());
                return new CommitCommandResponse(true, existingValue);
            }
            return new CommitCommandResponse(false, existingValue);
        }
        throw new IllegalArgumentException("Unknown command " + command);
    }

    private static Command getCommand(byte[] command) {
        return Command.deserialize(new ByteArrayInputStream(command));
    }

    private ExecuteCommandResponse handleExecute(ExecuteCommandRequest t) {
        ProposeRequest proposal = new ProposeRequest(getCommand(t.command).serialize());
        List<ProposeResponse> proposalResponses = blockingSendToReplicas(proposal.getRequestId(), proposal);
        if (proposalResponses.stream().filter(r -> r.isAccepted()).count() >= quorum()) {
            CommitCommandResponse c = sendCommitRequest(new CommitCommandRequest(getCommand(t.command).serialize()));
            return new ExecuteCommandResponse(c.getResponse(), c.isCommitted());
        };
        return ExecuteCommandResponse.notCommitted();
    }

    private CommitCommandResponse sendCommitRequest(CommitCommandRequest r) {
        List<CommitCommandResponse> proposalResponses = blockingSendToReplicas(r.getRequestId(), r);
        return proposalResponses.get(0);
    }

    public int quorum() {
        return getNoOfReplicas() / 2 + 1;
    }

    private ProposeResponse handlePropose(ProposeRequest proposeRequest) {
        acceptedCommand = getCommand(proposeRequest.getCommand());
        return new ProposeResponse(true);
    }

    public String getValue(String key) {
        return kvStore.get(key);
    }
}

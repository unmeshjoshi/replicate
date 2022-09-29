package distrib.patterns.leaderbasedpaxoslog;

import com.google.common.util.concurrent.Uninterruptibles;
import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.NIOSocketListener;
import distrib.patterns.net.SocketClient;
import distrib.patterns.net.SocketListener;
import distrib.patterns.net.requestwaitinglist.RequestCallback;
import distrib.patterns.net.requestwaitinglist.RequestWaitingList;
import distrib.patterns.common.MonotonicId;
import distrib.patterns.paxos.WriteTimeoutException;
import distrib.patterns.quorum.messages.GetValueRequest;
import distrib.patterns.quorum.messages.SetValueRequest;
import distrib.patterns.wal.Command;
import distrib.patterns.wal.EntryType;
import distrib.patterns.wal.SetValueCommand;
import distrib.patterns.wal.WALEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class PaxosState {
    MonotonicId promisedGeneration = MonotonicId.empty();
    Optional<MonotonicId> acceptedGeneration = Optional.empty();
    Optional<WALEntry> acceptedValue = Optional.empty();

    Optional<WALEntry> committedValue = Optional.empty();
    Optional<MonotonicId> committedGeneration = Optional.empty();

}

public class LeaderBasedPaxosLogClusterNode {
    private static Logger logger = LogManager.getLogger(LeaderBasedPaxosLogClusterNode.class);
    private final NIOSocketListener clientListener;
    private SystemClock clock;
    private Config config;
    private InetAddressAndPort clientConnectionAddress;
    private InetAddressAndPort peerConnectionAddress;
    private List<InetAddressAndPort> peers;
    private SocketListener listener;
    //Paxos State
    Map<Integer, PaxosState> paxosLog = new HashMap<>();

    Map<String, String> kv = new HashMap<>();

    public LeaderBasedPaxosLogClusterNode(SystemClock clock, Config config, InetAddressAndPort clientAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peers) throws IOException {
        this.clock = clock;
        this.config = config;
        this.clientConnectionAddress = clientAddress;
        this.peerConnectionAddress = peerConnectionAddress;
        this.peers = peers;
        this.listener = new SocketListener(this::handleServerMessage, peerConnectionAddress, config);
        RequestWaitingList waitingList = new RequestWaitingList(clock, Duration.ofMillis(8000));
        this.clientListener = new NIOSocketListener(message -> {
            RequestOrResponse request = message.getRequest();
            if (request.getRequestId() == RequestId.SetValueRequest.getId()) {
                var callback = new RequestCallback() {
                    @Override
                    public void onResponse(Object r, InetAddressAndPort address) {
                        message.getClientConnection().write(new RequestOrResponse(request.getRequestId(), JsonSerDes.serialize(r), request.getCorrelationId()));
                    }

                    @Override
                    public void onError(Exception e) {
                        message.getClientConnection().write(new RequestOrResponse(request.getRequestId(), JsonSerDes.serialize(e.getMessage()), request.getCorrelationId()));
                    }
                };
                waitingList.add(request.getCorrelationId(), callback);
                SetValueRequest setValueRequest = JsonSerDes.deserialize(request.getMessageBodyJson(), SetValueRequest.class);
                try {
                    SetValueCommand setValueCommand = new SetValueCommand(setValueRequest.getKey(), setValueRequest.getValue());
                    append(new WALEntry(0l, setValueCommand.serialize(), EntryType.DATA, 0));
                    callback.onResponse("Success", request.getFromAddress());
                } catch (WriteTimeoutException e) {
                    callback.onError(e);
                }

            } else if (request.getRequestId() == RequestId.GetValueRequest.getId()) {
                var callback = new RequestCallback() {
                    @Override
                    public void onResponse(Object r, InetAddressAndPort address) {
                        message.getClientConnection().write(new RequestOrResponse(request.getRequestId(), JsonSerDes.serialize(r), request.getCorrelationId()));
                    }

                    @Override
                    public void onError(Exception e) {
                        message.getClientConnection().write(new RequestOrResponse(request.getRequestId(), JsonSerDes.serialize(e.getMessage()), request.getCorrelationId()));
                    }
                };
                waitingList.add(request.getCorrelationId(), callback);
                GetValueRequest getValueRequest = JsonSerDes.deserialize(request.getMessageBodyJson(), GetValueRequest.class);
                append(new WALEntry(0l, new SetValueCommand("", "").serialize(), EntryType.DATA, 0)); //append a no-op command to make sure full paxos is run
                callback.onResponse(kv.get(getValueRequest.getKey()), request.getFromAddress());

            }
        }, clientAddress);


        requestWaitingList = new RequestWaitingList(clock);
    }

    RequestWaitingList requestWaitingList;
    int requestNumber;

    private int nextRequestId() {
        return requestNumber++;
    }

    int maxKnownPaxosRoundId = 1;
    int nextIndex = 0;
    int serverId = 1;
    int maxAttempts = 2;

    public boolean append(WALEntry command) {
        int attempts = 0;
        while(attempts <= maxAttempts) {
            attempts++;
            MonotonicId requestId = new MonotonicId(maxKnownPaxosRoundId++, serverId);
            PaxosResult paxosResult = doPaxos(requestId, nextIndex, command, peers);
            if (paxosResult.success) {
                return true;
            }
            Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), MILLISECONDS);
            logger.warn("Experienced Paxos contention. Attempting with higher generation");
            nextIndex++;
        }
        throw new WriteTimeoutException(attempts);

    }

    public InetAddressAndPort getClientConnectionAddress() {
        return clientConnectionAddress;
    }

    static class PaxosResult {
        Optional<WALEntry> value;
        boolean success;

        public PaxosResult(Optional<WALEntry> value, boolean success) {
            this.value = value;
            this.success = success;
        }
    }

    private PaxosResult doPaxos(MonotonicId monotonicId, int index, WALEntry proposedValue, List<InetAddressAndPort> replicas) {
        ProposalCallback proposalCallback = sendProposeRequest(index, proposedValue, monotonicId, replicas);
        if (proposalCallback.isQuorumAccepted()) {
            sendCommitRequest(index, proposedValue, monotonicId, replicas);
            return new PaxosResult(Optional.ofNullable(proposedValue), true);
        }
        return new PaxosResult(Optional.empty(), false);
    }

    private CommitCallback sendCommitRequest(int index, WALEntry proposedValue, MonotonicId monotonicId, List<InetAddressAndPort> replicas) {
        CommitCallback commitCallback = new CommitCallback(monotonicId);
        for (InetAddressAndPort replica : replicas) {
            int correlationId = nextRequestId();
            requestWaitingList.add(correlationId, commitCallback);
            try {
                SocketClient client = new SocketClient(replica);
                RequestOrResponse message = new RequestOrResponse(RequestId.CommitRequest.getId(), JsonSerDes.serialize(new CommitRequest(index, proposedValue, monotonicId)), correlationId, peerConnectionAddress);
                client.sendOneway(message);
            } catch (IOException e) {
                requestWaitingList.handleError(correlationId, e);
            }
        }
        return commitCallback;
    }

    private ProposalCallback sendProposeRequest(int index, WALEntry proposedValue, MonotonicId monotonicId, List<InetAddressAndPort> replicas) {
        ProposalCallback proposalCallback = new ProposalCallback(proposedValue);
        for (InetAddressAndPort replica : replicas) {
            int correlationId = nextRequestId();
            requestWaitingList.add(correlationId, proposalCallback);
            try {
                ProposalRequest proposalRequest = new ProposalRequest(monotonicId, index, proposedValue);
                RequestOrResponse message = new RequestOrResponse(RequestId.ProposeRequest.getId(), JsonSerDes.serialize(proposalRequest), correlationId, peerConnectionAddress);
                sendMessage(message, replica);
            } catch (Exception e) {
                requestWaitingList.handleError(correlationId, e);
            }
        }
        return proposalCallback;
    }

    static class PrepareCallback implements RequestCallback<RequestOrResponse> {
        CountDownLatch latch = new CountDownLatch(3);
        List<PrepareResponse> promises = new ArrayList<>();
        private WALEntry proposedValue;

        public PrepareCallback(WALEntry proposedValue) {
            this.proposedValue = proposedValue;
        }

        @Override
        public void onResponse(RequestOrResponse r, InetAddressAndPort address) {
            promises.add(JsonSerDes.deserialize(r.getMessageBodyJson(), PrepareResponse.class));
            latch.countDown();
        }

        @Override
        public void onError(Exception e) {
        }

        public WALEntry getProposedValue() {
            return getProposalValue(proposedValue, promises);
        }

        private WALEntry getProposalValue(WALEntry initialValue, List<PrepareResponse> promises) {
            PrepareResponse mostRecentAcceptedValue = getMostRecentAcceptedValue(promises);
            WALEntry proposedValue
                    = mostRecentAcceptedValue.acceptedValue.isEmpty() ?
                    initialValue : mostRecentAcceptedValue.acceptedValue.get();
            return proposedValue;
        }

        private PrepareResponse getMostRecentAcceptedValue(List<PrepareResponse> prepareResponses) {
            return prepareResponses.stream().max(Comparator.comparing(r -> r.acceptedGeneration.orElse(MonotonicId.empty()))).get();
        }

        public boolean isQuorumPrepared() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return true;
        }
    }

    private PrepareCallback sendPrepareRequest(int index, WALEntry proposedValue, MonotonicId monotonicId, List<InetAddressAndPort> replicas) {
        PrepareCallback prepareCallback = new PrepareCallback(proposedValue);
        for (InetAddressAndPort replica : replicas) {
            int correlationId = nextRequestId();
            requestWaitingList.add(correlationId, prepareCallback);
            try {
                RequestOrResponse message = new RequestOrResponse(RequestId.PrepareRequest.getId(),
                        JsonSerDes.serialize(new PrepareRequest(index, monotonicId)), correlationId, peerConnectionAddress);
                sendMessage(message, replica);

            } catch (Exception e) {
                requestWaitingList.handleError(correlationId, e);
            }
        }
        return prepareCallback;
    }

    public void start() {
        listener.start();
        clientListener.start();
    }

    public void runElection() {
        this.fullLogPromisedGeneration = new MonotonicId(maxKnownPaxosRoundId++, serverId);
        FullLogPrepareCallback fullLogPrepareCallback = sendFullLogPrepare(fullLogPromisedGeneration);
        if (fullLogPrepareCallback.isQuorumPrepared()) {
            isLeader = true;
            List<FullLogPrepareResponse> promises = fullLogPrepareCallback.promises;
            for (FullLogPrepareResponse promise : promises) {
                mergeLog(promise);
                sendProposalRequestsForUnCommittedEntries();
            }
        }
    }

    boolean isLeader = false;
    private void sendProposalRequestsForUnCommittedEntries() {
        Map<Integer, PaxosState> uncommitedValues = getUncommitedValues();
        for (Integer index : uncommitedValues.keySet()) {
            PaxosState logEntry = uncommitedValues.get(index);
            WALEntry proposedValue = logEntry.acceptedValue.get();
            ProposalCallback proposalCallback = sendProposeRequest(index, proposedValue, fullLogPromisedGeneration, peers);
            if (!proposalCallback.isQuorumAccepted()) {
                isLeader = false;
            }
            sendCommitRequest(index, proposedValue, fullLogPromisedGeneration, peers);
        }
    }




    private void mergeLog(FullLogPrepareResponse promise) {
        var indexes = promise.uncommittedValues.keySet();
        for (Integer index : indexes) {
            PaxosState peerEntry = promise.uncommittedValues.get(index);
            PaxosState selfEntry = paxosLog.get(index);
            if (selfEntry == null || isAfter(peerEntry.acceptedGeneration, selfEntry.acceptedGeneration)) {
                paxosLog.put(index, peerEntry);
            }
        }
    }

    private boolean isAfter(Optional<MonotonicId> m1, Optional<MonotonicId> m2) {
        if (m1.isPresent() && m2.isPresent()) {
            return m1.get().isAfter(m2.get());
        }
        if (m1.isPresent()) {
            return true;
        }

        if (m2.isPresent()) {
            return false;
        }

        return false;
    }

    private FullLogPrepareCallback sendFullLogPrepare(MonotonicId fullLogPromisedGeneration) {
        FullLogPrepareCallback prepareCallback = new FullLogPrepareCallback();
        for (InetAddressAndPort replica : peers) {
            int correlationId = nextRequestId();
            requestWaitingList.add(correlationId, prepareCallback);
            try {
                RequestOrResponse message = new RequestOrResponse(RequestId.PrepareRequest.getId(),
                        JsonSerDes.serialize(new PrepareRequest(-1, fullLogPromisedGeneration)), correlationId, peerConnectionAddress);
                sendMessage(message, replica);

            } catch (Exception e) {
                requestWaitingList.handleError(correlationId, e);
            }
        }
        return prepareCallback;
    }

    private synchronized void handleServerMessage(Message<RequestOrResponse> message) {
        RequestOrResponse requestOrResponse = message.getRequest();
        if (requestOrResponse.getRequestId() == RequestId.PrepareRequest.getId()) {
            handleFullLogPrepare(requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.FullLogPrepareResponse.getId()) {
            requestWaitingList.handleResponse(requestOrResponse.getCorrelationId(), requestOrResponse, requestOrResponse.getFromAddress());

        } else if (requestOrResponse.getRequestId() == RequestId.PrepareRequest.getId()) {
            handlePaxosPrepare(requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.Promise.getId()) {
            requestWaitingList.handleResponse(requestOrResponse.getCorrelationId(), requestOrResponse, requestOrResponse.getFromAddress() );

        } else if (requestOrResponse.getRequestId() == RequestId.ProposeRequest.getId()) {
            handlePaxosProposal(requestOrResponse.getCorrelationId(), requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.ProposeResponse.getId()) {
            requestWaitingList.handleResponse(requestOrResponse.getCorrelationId(), requestOrResponse, requestOrResponse.getFromAddress());

        } else if (requestOrResponse.getRequestId() == RequestId.CommitRequest.getId()) {
            handlePaxosCommit(requestOrResponse.getCorrelationId(), requestOrResponse);
        }
    }

    private void handlePaxosCommit(Integer correlationId, RequestOrResponse requestOrResponse) {
        CommitRequest request = JsonSerDes.deserialize(requestOrResponse.getMessageBodyJson(), CommitRequest.class);
        PaxosState paxosState = getOrCreatePaxosState(request.getIndex());
        paxosState.committedGeneration = Optional.of(request.getMonotonicId());
        paxosState.committedValue = Optional.of(request.getProposedValue());
        addAndApplyIfAllThePreviousEntriesAreCommitted(request);
        sendMessage(new RequestOrResponse(RequestId.CommitResponse.getId(), JsonSerDes.serialize(true), correlationId), requestOrResponse.getFromAddress());
    }

    private void addAndApplyIfAllThePreviousEntriesAreCommitted(CommitRequest commitRequest) {
        //if all entries upto logIndex - 1 are committed, apply this entry.
        List<Integer> previousIndexes = this.paxosLog.keySet().stream().filter(index -> index < commitRequest.getIndex()).collect(Collectors.toList());
        boolean allPreviousCommitted = true;
        for (Integer previousIndex : previousIndexes) {
            if (paxosLog.get(previousIndex).committedValue.isEmpty()) {
                allPreviousCommitted = false;
                break;
            }
        }
        if (allPreviousCommitted) {
            addAndApply(commitRequest.getProposedValue());
        }

        //see if there are entries above this logIndex which are commited, apply those entries.
        for(long startIndex = commitRequest.getIndex() + 1; ;startIndex++) {
            PaxosState paxosState = paxosLog.get(startIndex);
            if (paxosState == null) {
                break;
            }
            WALEntry committed = paxosState.committedValue.get();
            addAndApply(committed);
        }
    }

    private void addAndApply(WALEntry walEnty) {
        Command command = Command.deserialize(new ByteArrayInputStream(walEnty.getData()));
        if (command instanceof SetValueCommand) {
            SetValueCommand setValueCommand = (SetValueCommand)command;
            kv.put(setValueCommand.getKey(), setValueCommand.getValue());
        }
    }

    private void handlePaxosProposal(Integer correlationId, RequestOrResponse requestOrResponse) {
        ProposalRequest request = JsonSerDes.deserialize(requestOrResponse.getMessageBodyJson(), ProposalRequest.class);
        MonotonicId generation = request.getMonotonicId();
        PaxosState paxosState = getOrCreatePaxosState(request.getIndex());
        if (generation.equals(paxosState.promisedGeneration) || generation.isAfter(paxosState.promisedGeneration)) {
            paxosState.promisedGeneration = generation;
            paxosState.acceptedGeneration = Optional.of(generation);
            paxosState.acceptedValue = Optional.ofNullable(request.getProposedValue());
            sendMessage(new RequestOrResponse(RequestId.ProposeResponse.getId(), JsonSerDes.serialize(true), correlationId), requestOrResponse.getFromAddress());
        }
    }

    MonotonicId fullLogPromisedGeneration = MonotonicId.empty();

    private void handleFullLogPrepare(RequestOrResponse requestOrResponse) {
        FullLogPrepareResponse fullLogPrepareResponse = fullLogPrepare(requestOrResponse);
        sendMessage(new RequestOrResponse(RequestId.FullLogPrepareResponse.getId(), JsonSerDes.serialize(fullLogPrepareResponse), requestOrResponse.getCorrelationId()), requestOrResponse.getFromAddress());
    }

    private FullLogPrepareResponse fullLogPrepare(RequestOrResponse requestOrResponse) {
        PrepareRequest request = JsonSerDes.deserialize(requestOrResponse.getMessageBodyJson(), PrepareRequest.class);
        MonotonicId generation = request.monotonicId;
        if (fullLogPromisedGeneration.isAfter(generation)) {
            return new FullLogPrepareResponse(false, Collections.EMPTY_MAP);
        }
        fullLogPromisedGeneration = generation;
        return new FullLogPrepareResponse(true, getUncommitedValues());
    }


    private Map<Integer, PaxosState> getUncommitedValues() {
        Map<Integer, PaxosState> uncommittedEntries = new HashMap<>();
        Set<Integer> indexes = paxosLog.keySet();
        for (Integer index : indexes) {
            PaxosState paxosState = paxosLog.get(index);
            if (paxosState.committedValue.isPresent()) {
                uncommittedEntries.put(index, paxosState);
            }
        }
        return uncommittedEntries;
    }

    private void handlePaxosPrepare(RequestOrResponse requestOrResponse) {
        PrepareRequest request = JsonSerDes.deserialize(requestOrResponse.getMessageBodyJson(), PrepareRequest.class);
        PrepareResponse prepareResponse = prepare(request.index, request.monotonicId);
        sendMessage(new RequestOrResponse(RequestId.Promise.getId(), JsonSerDes.serialize(prepareResponse), requestOrResponse.getCorrelationId()), requestOrResponse.getFromAddress());
    }

    public PrepareResponse prepare(int index, MonotonicId generation) {
        PaxosState paxosState = getOrCreatePaxosState(index);
        if (paxosState.promisedGeneration.isAfter(generation)) {
            return new PrepareResponse(false, paxosState.acceptedValue, paxosState.acceptedGeneration);
        }
        paxosState.promisedGeneration = generation;
        return new PrepareResponse(true, paxosState.acceptedValue, paxosState.acceptedGeneration);
    }

    private PaxosState getOrCreatePaxosState(int index) {
        PaxosState paxosState = paxosLog.get(index);
        if (paxosState == null) {
            paxosState = new PaxosState();
            paxosLog.put(index, paxosState);
        }
        return paxosState;
    }

    private void handleResponse(RequestOrResponse response) {
        requestWaitingList.handleResponse(response.getCorrelationId(), response);
    }

    private void handleSetValueRequest(RequestOrResponse request) {
        SetValueRequest setValueRequest = deserialize(request);
        sendMessage(new RequestOrResponse(request.getGeneration(), RequestId.SetValueResponse.getId(), setValueRequest.getValue().getBytes(), request.getCorrelationId(), peerConnectionAddress), request.getFromAddress());
    }

//    static class Network {
//        List<InetAddressAndPort> dropMessagesTo = new ArrayList<>();
//        public void dropMessagesTo(InetAddressAndPort address) {
//            dropMessagesTo.add(address);
//        }
//        public void sendOneWay(InetAddressAndPort address, RequestOrResponse requestOrResponse) {
//            if (dropMessagesTo.contains(address)) {
//                return;
//            }
//            try {
//
//                SocketClient socketClient = new SocketClient(address);
//            socketClient.sendOneway(requestOrResponse);
//        }
//    }

    private void sendMessage(RequestOrResponse message, InetAddressAndPort toAddress) {
        try {
            if (toAddress.equals(peerConnectionAddress)) {
                handleServerMessage(new Message<RequestOrResponse>(message, RequestId.valueOf(message.getRequestId())));
                return;
            }
            SocketClient socketClient = new SocketClient(toAddress);
            socketClient.sendOneway(message);
        } catch (IOException e) {
            logger.error("Communication failure sending request to " + toAddress);
            throw new RuntimeException(e);
        }
    }

    private SetValueRequest deserialize(RequestOrResponse request) {
        return JsonSerDes.deserialize(request.getMessageBodyJson(), SetValueRequest.class);
    }
}

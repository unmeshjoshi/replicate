package distrib.patterns.common;

import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.requestwaitinglist.RequestCallback;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class AsyncQuorumCallback<T> implements RequestCallback<T> {
    final int quorum;
    private final int totalResponses;
    List<Exception> exceptions = new ArrayList<>();
    Map<InetAddressAndPort, T> responses = new HashMap<>();
    CompletableFuture<Map<InetAddressAndPort, T>> quorumFuture = new CompletableFuture<>();

    public AsyncQuorumCallback(int totalResponses) {
        assert totalResponses > 0;
        this.totalResponses = totalResponses;
        this.quorum = totalResponses / 2 + 1;
    }

    @Override
    public void onResponse(T r, InetAddressAndPort fromAddress) {
        responses.put(fromAddress, r);
        if (responses.size() == quorum) {
            quorumFuture.complete(responses);
        }
    }

    @Override
    public void onError(Exception e) {
        exceptions.add(e);
        if (exceptions.size() == quorum) {
            quorumFuture.completeExceptionally(new QuorumCallException(exceptions));
        }
    }

    public CompletableFuture<Map<InetAddressAndPort, T>> getQuorumFuture() {
        return quorumFuture;
    }
}

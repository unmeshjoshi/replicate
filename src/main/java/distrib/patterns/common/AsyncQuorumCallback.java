package distrib.patterns.common;

import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.requestwaitinglist.RequestCallback;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public class AsyncQuorumCallback<T> implements RequestCallback<T> {
    private final int totalResponses;
    List<Exception> exceptions = new ArrayList<>();
    Map<InetAddressAndPort, T> responses = new HashMap<>();
    CompletableFuture<Map<InetAddressAndPort, T>> quorumFuture = new CompletableFuture<>();
    private Predicate<T> successCondition;

    public AsyncQuorumCallback(int totalResponses) {
        //This is default implementation. But its good to provide a specific quorum condition.
        this(totalResponses, (responses)->true);
    }

    public AsyncQuorumCallback(int totalResponses, Predicate<T> successCondition) {
        this.successCondition = successCondition;
        assert totalResponses > 0;
        this.totalResponses = totalResponses;
    }

    private int majorityQuorum() {
        return totalResponses / 2 + 1;
    }

    @Override
    public void onResponse(T r, InetAddressAndPort fromAddress) {
        responses.put(fromAddress, r);
        if (quorumSucceeded(responses)) {
            quorumFuture.complete(responses);
        }
        if (responses.size() == totalResponses) {
            quorumFuture.completeExceptionally(new RuntimeException("Quorum condition not met after " + totalResponses + " responses"));
        }
    }

    private boolean quorumSucceeded(Map<InetAddressAndPort, T> r) {
        return r.values()
                .stream()
                .filter(successCondition).count() >= majorityQuorum();
    }

    @Override
    public void onError(Exception e) {
        exceptions.add(e);
        if (exceptions.size() == majorityQuorum()) {
            quorumFuture.completeExceptionally(new QuorumCallException(exceptions));
        }
    }

    public CompletableFuture<Map<InetAddressAndPort, T>> getQuorumFuture() {
        return quorumFuture;
    }
}

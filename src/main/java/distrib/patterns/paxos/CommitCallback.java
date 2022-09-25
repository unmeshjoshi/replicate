package distrib.patterns.paxos;

import distrib.patterns.common.JsonSerDes;
import distrib.patterns.common.MonotonicId;
import distrib.patterns.common.RequestOrResponse;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.requestwaitinglist.RequestCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class CommitCallback implements RequestCallback<RequestOrResponse> {
    private MonotonicId monotonicId;

    public CommitCallback(MonotonicId monotonicId) {
        this.monotonicId = monotonicId;
    }

    public MonotonicId getMonotonicId() {
        return monotonicId;
    }


    CountDownLatch latch = new CountDownLatch(3);
    List<CommitResponse> proposalResponses = new ArrayList<>();

    @Override
    public void onResponse(RequestOrResponse r, InetAddressAndPort address) {
        proposalResponses.add(JsonSerDes.deserialize(r.getMessageBodyJson(), CommitResponse.class));
        latch.countDown();
    }

    @Override
    public void onError(Exception e) {
    }

    public boolean isQuorumAccepted() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            //TODO
            e.printStackTrace();
        }
        return true;
    }
}

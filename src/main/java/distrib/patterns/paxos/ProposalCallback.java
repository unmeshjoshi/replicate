package distrib.patterns.paxos;

import distrib.patterns.common.JsonSerDes;
import distrib.patterns.common.RequestOrResponse;
import distrib.patterns.net.requestwaitinglist.RequestCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ProposalCallback implements RequestCallback<RequestOrResponse> {
    private String proposedValue;

    public ProposalCallback(String proposedValue) {
        this.proposedValue = proposedValue;
    }

    CountDownLatch latch = new CountDownLatch(3);
    List<ProposalResponse> proposalResponses = new ArrayList<>();

    @Override
    public void onResponse(RequestOrResponse r) {
        proposalResponses.add(JsonSerDes.deserialize(r.getMessageBodyJson(), ProposalResponse.class));
        latch.countDown();
    }

    @Override
    public void onError(Throwable e) {
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

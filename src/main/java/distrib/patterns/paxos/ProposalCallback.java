package distrib.patterns.paxos;

import com.google.common.util.concurrent.Uninterruptibles;
import distrib.patterns.common.JsonSerDes;
import distrib.patterns.common.RequestOrResponse;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.requestwaitinglist.RequestCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ProposalCallback implements RequestCallback<RequestOrResponse> {
    private String proposedValue;
    int clusterSize = 3; //TODO: make constructor param
    int quorum = clusterSize / 2 + 1;
    public ProposalCallback(String proposedValue) {
        this.proposedValue = proposedValue;
    }

    CountDownLatch latch = new CountDownLatch(3);
    List<ProposalResponse> proposalResponses = new ArrayList<>();

    @Override
    public void onResponse(RequestOrResponse r, InetAddressAndPort address) {
        proposalResponses.add(JsonSerDes.deserialize(r.getMessageBodyJson(), ProposalResponse.class));
        latch.countDown();
    }

    @Override
    public void onError(Exception e) {
    }

    public boolean isQuorumAccepted() {
        Uninterruptibles.awaitUninterruptibly(latch);
        return proposalResponses.stream().filter(p -> p.success).count() >= quorum;
    }
}

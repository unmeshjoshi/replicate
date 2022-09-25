package distrib.patterns.paxoslog;

import distrib.patterns.common.JsonSerDes;
import distrib.patterns.common.RequestOrResponse;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.requestwaitinglist.RequestCallback;
import distrib.patterns.paxos.ProposalResponse;
import distrib.patterns.wal.Command;
import distrib.patterns.wal.WALEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ProposalCallback implements RequestCallback<RequestOrResponse> {
    private WALEntry proposedValue;

    public ProposalCallback(WALEntry proposedValue) {
        this.proposedValue = proposedValue;
    }

    CountDownLatch latch = new CountDownLatch(3);
    List<ProposalResponse> proposalResponses = new ArrayList<>();
    List<Throwable> exceptions = new ArrayList<>();
    @Override
    public void onResponse(RequestOrResponse r, InetAddressAndPort address) {
        proposalResponses.add(JsonSerDes.deserialize(r.getMessageBodyJson(), ProposalResponse.class));
        latch.countDown();
    }

    @Override
    public void onError(Exception e) {
        exceptions.add(e);
        latch.countDown();

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

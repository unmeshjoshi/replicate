package distrib.patterns.paxos;

import com.google.common.util.concurrent.Uninterruptibles;
import distrib.patterns.common.BlockingQuorumCallback;
import distrib.patterns.common.JsonSerDes;
import distrib.patterns.common.RequestOrResponse;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.requestwaitinglist.RequestCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ProposalCallback extends BlockingQuorumCallback<ProposalResponse> {
    public ProposalCallback(int clusterSize) {
        super(clusterSize);
    }

    public boolean isQuorumAccepted() {
        return blockAndGetQuorumResponses().values().stream().filter(p -> p.success).count() >= quorum;
    }
}

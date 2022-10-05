package distrib.patterns.paxos;

import distrib.patterns.common.BlockingQuorumCallback;

public class ProposalCallback extends BlockingQuorumCallback<ProposalResponse> {
    public ProposalCallback(int clusterSize) {
        super(clusterSize);
    }

    public boolean isQuorumAccepted() {
        return blockAndGetQuorumResponses().values().stream().filter(p -> p.success).count() >= quorum;
    }
}

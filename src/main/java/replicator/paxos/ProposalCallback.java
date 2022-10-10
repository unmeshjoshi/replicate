package replicator.paxos;

import replicator.common.BlockingQuorumCallback;
import replicator.paxos.messages.ProposalResponse;

public class ProposalCallback extends BlockingQuorumCallback<ProposalResponse> {
    public ProposalCallback(int clusterSize) {
        super(clusterSize);
    }

    public boolean isQuorumAccepted() {
        return blockAndGetQuorumResponses().values().stream().filter(p -> p.success).count() >= quorum;
    }
}

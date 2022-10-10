package replicate.paxos;

import replicate.common.BlockingQuorumCallback;
import replicate.paxos.messages.ProposalResponse;

public class ProposalCallback extends BlockingQuorumCallback<ProposalResponse> {
    public ProposalCallback(int clusterSize) {
        super(clusterSize);
    }

    public boolean isQuorumAccepted() {
        return blockAndGetQuorumResponses().values().stream().filter(p -> p.success).count() >= quorum;
    }
}

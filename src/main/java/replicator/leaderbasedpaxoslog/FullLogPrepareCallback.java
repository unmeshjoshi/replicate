package replicator.leaderbasedpaxoslog;

import replicator.common.BlockingQuorumCallback;
import replicator.leaderbasedpaxoslog.messages.FullLogPrepareResponse;

public class FullLogPrepareCallback extends BlockingQuorumCallback<FullLogPrepareResponse> {

    public FullLogPrepareCallback(int totalResponses) {
        super(totalResponses);
    }


    public boolean isQuorumPrepared() {
        return blockAndGetQuorumResponses()
                .values()
                .stream()
                .filter(p -> p.promised).count() >= quorum;
    }
}

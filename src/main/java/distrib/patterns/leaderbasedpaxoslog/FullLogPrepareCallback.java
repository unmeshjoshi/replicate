package distrib.patterns.leaderbasedpaxoslog;

import distrib.patterns.common.BlockingQuorumCallback;
import distrib.patterns.leaderbasedpaxoslog.messages.FullLogPrepareResponse;

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

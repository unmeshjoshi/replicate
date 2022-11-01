package replicate.multipaxos;

import replicate.common.BlockingQuorumCallback;
import replicate.multipaxos.messages.FullLogPrepareResponse;

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

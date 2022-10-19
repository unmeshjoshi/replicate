package replicate.generationvoting.messages;

import replicate.common.Request;
import replicate.common.RequestId;

public class PrepareRequest extends Request {
    public final int proposedBallot;

    public PrepareRequest(int proposedBallot) {
        super(RequestId.Prepare);
        this.proposedBallot = proposedBallot;
    }

    public int getProposedBallot() {
        return proposedBallot;
    }
}

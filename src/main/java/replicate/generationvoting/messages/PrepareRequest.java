package replicate.generationvoting.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class PrepareRequest extends MessagePayload {
    public final int proposedBallot;

    public PrepareRequest(int proposedBallot) {
        super(MessageId.Prepare);
        this.proposedBallot = proposedBallot;
    }

    public int getProposedBallot() {
        return proposedBallot;
    }
}

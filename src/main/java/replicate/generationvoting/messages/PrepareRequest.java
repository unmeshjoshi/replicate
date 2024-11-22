package replicate.generationvoting.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class PrepareRequest extends MessagePayload {
    public final int proposedGeneration;

    public PrepareRequest(int proposedBallot) {
        super(MessageId.Prepare);
        this.proposedGeneration = proposedBallot;
    }
}

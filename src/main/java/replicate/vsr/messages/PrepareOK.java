package replicate.vsr.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class PrepareOK extends MessagePayload {
    public final int viewNumber;
    public final int opNumber;
    public final int replicaIndex;
    public final boolean isAck;

    public PrepareOK(int viewNumber, int opNumber, int replicaIndex, boolean isAck) {
        super(MessageId.PrepareOK);
        this.viewNumber = viewNumber;
        this.opNumber = opNumber;
        this.replicaIndex = replicaIndex;
        this.isAck = isAck;
    }
}

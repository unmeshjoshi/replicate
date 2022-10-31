package replicate.vsr.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class StartViewChange extends MessagePayload {
    public final MessageId startViewChange;
    public final int viewNumber;
    public final int replicaIndex;

    public StartViewChange(MessageId startViewChange, int viewNumber, int replicaIndex) {
        super(MessageId.StartViewChange);

        this.startViewChange = startViewChange;
        this.viewNumber = viewNumber;
        this.replicaIndex = replicaIndex;
    }
}

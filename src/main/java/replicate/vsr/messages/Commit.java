package replicate.vsr.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class Commit extends MessagePayload {
    public final int viewNumber;
    public final int commitNumber;

    public Commit(int viewNumber, int commitNumber) {
        super(MessageId.Commit);
        this.viewNumber = viewNumber;
        this.commitNumber = commitNumber;
    }
}

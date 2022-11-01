package replicate.paxoslog.messages;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class CommitRequest extends MessagePayload {
    public final int index;
    public final byte[] committedValue;
    public final MonotonicId generation;

    public CommitRequest(int index, byte[] committedValue, MonotonicId generation) {
        super(MessageId.Commit);
        this.index = index;
        this.committedValue = committedValue;
        this.generation = generation;
    }
}

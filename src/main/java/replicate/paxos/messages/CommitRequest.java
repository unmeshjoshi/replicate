package replicate.paxos.messages;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class CommitRequest extends MessagePayload {
    public final MonotonicId generation;
    public final byte[] value;
    public CommitRequest(MonotonicId generation, byte[] value) {
        super(MessageId.Commit);
        this.generation = generation;
        this.value = value;
    }
}

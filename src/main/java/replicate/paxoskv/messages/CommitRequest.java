package replicate.paxoskv.messages;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class CommitRequest extends MessagePayload {
    public final String key;
    public final byte[] value;
    public final MonotonicId generation;

    public CommitRequest(String key, byte[] value, MonotonicId generation) {
        super(MessageId.Commit);
        this.key = key;
        this.value = value;
        this.generation = generation;
    }
}

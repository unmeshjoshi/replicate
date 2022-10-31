package replicate.paxos.messages;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class CommitRequest extends MessagePayload {
    private MonotonicId generation;
    private byte[] value;
    public CommitRequest(MonotonicId generation, byte[] value) {
        this();
        this.generation = generation;
        this.value = value;
    }

    public MonotonicId getGeneration() {
        return generation;
    }

    public byte[] getValue() {
        return value;
    }

    private CommitRequest() {
        super(MessageId.Commit);
    }
}

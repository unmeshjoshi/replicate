package replicate.chain.messages;

import replicate.common.MessageId;
import replicate.common.MessagePayload;

import java.util.UUID;

public class ChainWriteAck extends MessagePayload {
    private final UUID requestId;
    private final int version;
    private final boolean success;

    public ChainWriteAck(UUID requestId) {
        super(MessageId.ChainWriteAck);
        this.requestId = requestId;
        this.version = 0;
        this.success = true;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public int getVersion() {
        return version;
    }

    public boolean isSuccess() {
        return success;
    }
} 
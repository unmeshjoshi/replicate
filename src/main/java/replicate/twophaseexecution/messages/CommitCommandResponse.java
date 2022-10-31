package replicate.twophaseexecution.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

import java.util.Optional;

public class CommitCommandResponse extends MessagePayload {
    boolean committed;
    Optional<String> response;
    public CommitCommandResponse(boolean committed, Optional<String> response) {
        this();
        this.committed = committed;
        this.response = response;
    }

    public boolean isCommitted() {
        return committed;
    }

    public Optional<String> getResponse() {
        return response;
    }

    private CommitCommandResponse() {
        super(MessageId.CommitResponse);
    }
}

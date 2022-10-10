package replicate.twophaseexecution.messages;

import replicate.common.Request;
import replicate.common.RequestId;

import java.util.Optional;

public class CommitCommandResponse extends Request {
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
        super(RequestId.CommitResponse);
    }
}

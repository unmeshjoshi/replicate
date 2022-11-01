package replicate.twophaseexecution.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

import java.util.Optional;

public class ExecuteCommandResponse extends MessagePayload {
    Optional<String> response;
    boolean isCommitted;
    public ExecuteCommandResponse(Optional<String> response, boolean isCommitted) {
        this();
        this.response = response;
        this.isCommitted = isCommitted;
    }

    public static ExecuteCommandResponse notCommitted() {
        return new ExecuteCommandResponse(Optional.empty(), false);
    }

    public static ExecuteCommandResponse errorResponse(String s) {
        return new ExecuteCommandResponse(Optional.of(s), false);
    }

    public Optional<String> getResponse() {
        return response;
    }

    public boolean isCommitted() {
        return isCommitted;
    }

    private ExecuteCommandResponse() {
        super(MessageId.ExcuteCommandResponse);
    }
}

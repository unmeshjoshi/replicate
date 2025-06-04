package replicate.chain.messages;

import replicate.common.MessageId;
import replicate.common.MessagePayload;

import java.util.Optional;

public class ExecuteCommandResponse extends MessagePayload {
    private final Optional<String> response;
    private final boolean isCommitted;

    public ExecuteCommandResponse(Optional<String> response, boolean isCommitted) {
        super(MessageId.ExcuteCommandResponse);
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
} 
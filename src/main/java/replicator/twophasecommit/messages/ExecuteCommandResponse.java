package replicator.twophasecommit.messages;

import replicator.common.Request;
import replicator.common.RequestId;

import java.util.Optional;

public class ExecuteCommandResponse extends Request {
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
        super(RequestId.ExcuteCommandResponse);
    }
}

package distrib.patterns.twophasecommit;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;
import distrib.patterns.wal.Command;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

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

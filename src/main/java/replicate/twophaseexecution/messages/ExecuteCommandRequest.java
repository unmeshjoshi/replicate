package replicate.twophaseexecution.messages;

import replicate.common.Request;
import replicate.common.RequestId;

public class ExecuteCommandRequest extends Request {
    public final byte[] command;

    public ExecuteCommandRequest(byte[] command) {
        super(RequestId.ExcuteCommandRequest);
        this.command = command;
    }
}

package replicate.twophaseexecution.messages;

import replicate.common.Request;
import replicate.common.RequestId;

public class ProposeRequest extends Request {
    byte[] command;
    public ProposeRequest(byte[] serialize) {
        this();
        this.command = serialize;
    }

    public byte[] getCommand() {
        return command;
    }

    private ProposeRequest() {
        super(RequestId.ProposeRequest);
    }
}

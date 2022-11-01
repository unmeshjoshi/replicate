package replicate.vsr.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;
import replicate.twophaseexecution.messages.ExecuteCommandRequest;

public class Prepare extends MessagePayload {
    public final int viewNumber;
    public final ExecuteCommandRequest request;
    public final int opNumber;
    public final int commitNumber;

    public Prepare(int viewNumber, ExecuteCommandRequest request, int opNumber, int commitNumber) {
        super(MessageId.Prepare);
        this.viewNumber = viewNumber;
        this.request = request;
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
    }
}

package replicate.vsr.messages;

import replicate.common.Request;
import replicate.common.RequestId;
import replicate.twophaseexecution.messages.ExecuteCommandRequest;

public class Prepare extends Request {
    public final int viewNumber;
    public final ExecuteCommandRequest request;
    public final int opNumber;
    public final int commitNumber;

    public Prepare(int viewNumber, ExecuteCommandRequest request, int opNumber, int commitNumber) {
        super(RequestId.Prepare);
        this.viewNumber = viewNumber;
        this.request = request;
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
    }
}

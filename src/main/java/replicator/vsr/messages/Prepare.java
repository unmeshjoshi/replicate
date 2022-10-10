package replicator.vsr.messages;

import replicator.common.Request;
import replicator.common.RequestId;
import replicator.twophasecommit.messages.ExecuteCommandRequest;

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

package distrib.patterns.vsr.messages;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;
import distrib.patterns.twophasecommit.messages.ExecuteCommandRequest;

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

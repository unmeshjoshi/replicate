package distrib.patterns.vsr.messages;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

public class Commit extends Request {
    public final int viewNumber;
    public final int commitNumber;

    public Commit(int viewNumber, int commitNumber) {
        super(RequestId.Commit);
        this.viewNumber = viewNumber;
        this.commitNumber = commitNumber;
    }
}

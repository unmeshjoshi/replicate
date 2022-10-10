package replicator.vsr.messages;

import replicator.common.Request;
import replicator.common.RequestId;

public class PrepareOK extends Request {
    public final int viewNumber;
    public final int opNumber;
    public final int replicaIndex;
    public final boolean isAck;

    public PrepareOK(int viewNumber, int opNumber, int replicaIndex, boolean isAck) {
        super(RequestId.PrepareOK);
        this.viewNumber = viewNumber;
        this.opNumber = opNumber;
        this.replicaIndex = replicaIndex;
        this.isAck = isAck;
    }
}

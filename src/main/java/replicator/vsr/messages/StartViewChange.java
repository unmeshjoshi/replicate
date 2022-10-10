package replicator.vsr.messages;

import replicator.common.Request;
import replicator.common.RequestId;

public class StartViewChange extends Request {
    public final RequestId startViewChange;
    public final int viewNumber;
    public final int replicaIndex;

    public StartViewChange(RequestId startViewChange, int viewNumber, int replicaIndex) {
        super(RequestId.StartViewChange);

        this.startViewChange = startViewChange;
        this.viewNumber = viewNumber;
        this.replicaIndex = replicaIndex;
    }
}

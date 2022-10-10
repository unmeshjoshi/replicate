package replicator.leaderbasedpaxoslog.messages;

import replicator.common.Request;
import replicator.common.RequestId;
import replicator.leaderbasedpaxoslog.PaxosState;

import java.util.Map;


public class FullLogPrepareResponse extends Request {
    public final boolean promised;
    public final Map<Integer, PaxosState> uncommittedValues;

    public FullLogPrepareResponse(boolean promised, Map<Integer, PaxosState> uncommittedValues) {
        super(RequestId.Promise);
        this.promised = promised;
        this.uncommittedValues = uncommittedValues;
    }
}

package replicate.multipaxos.messages;

import replicate.common.Request;
import replicate.common.RequestId;
import replicate.multipaxos.PaxosState;

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

package distrib.patterns.leaderbasedpaxoslog;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

import java.util.Map;

class FullLogPrepareResponse extends Request {
    public final boolean promised;
    public final Map<Integer, PaxosState> uncommittedValues;

    public FullLogPrepareResponse(boolean promised, Map<Integer, PaxosState> uncommittedValues) {
        super(RequestId.Promise);
        this.promised = promised;
        this.uncommittedValues = uncommittedValues;
    }
}

package distrib.patterns.leaderbasedpaxoslog.messages;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;
import distrib.patterns.leaderbasedpaxoslog.PaxosState;
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

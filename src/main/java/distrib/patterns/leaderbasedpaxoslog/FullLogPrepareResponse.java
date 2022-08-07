package distrib.patterns.leaderbasedpaxoslog;

import java.util.Map;

class FullLogPrepareResponse {
    boolean accepted;
    Map<Integer, PaxosState> uncommittedValues;

    public FullLogPrepareResponse(boolean accepted, Map<Integer, PaxosState> uncommittedValues) {
        this.accepted = accepted;
        this.uncommittedValues = uncommittedValues;
    }

    //for jackson
    private FullLogPrepareResponse() {

    }
}

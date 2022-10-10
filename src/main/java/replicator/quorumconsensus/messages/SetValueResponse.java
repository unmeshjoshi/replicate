package replicator.quorumconsensus.messages;

import replicator.common.Request;
import replicator.common.RequestId;

public class SetValueResponse extends Request {
    String result;

    public SetValueResponse(String result) {
        this();
        this.result = result;
    }

    private SetValueResponse() {
        super(RequestId.SetValueResponse);
    }

    public String getResult() {
        return result;
    }
}

package replicator.quorum.messages;

import replicator.common.Request;
import replicator.common.RequestId;

public class SetValueResponse extends Request {
    public final String result;

    public SetValueResponse(String result) {
        super(RequestId.SetValueResponse);
        this.result = result;
    }
}

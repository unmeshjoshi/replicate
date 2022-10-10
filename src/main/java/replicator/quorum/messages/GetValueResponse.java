package replicator.quorum.messages;

import replicator.common.Request;
import replicator.common.RequestId;
import replicator.quorum.StoredValue;

public class GetValueResponse extends Request {
    StoredValue value;

    public GetValueResponse(StoredValue value) {
        this();
        this.value = value;
    }

    private GetValueResponse() {
        super(RequestId.GetValueResponse);
    }

    public StoredValue getValue() {
        return value;
    }
}

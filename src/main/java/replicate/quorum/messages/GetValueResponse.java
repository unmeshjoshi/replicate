package replicate.quorum.messages;

import replicate.common.Request;
import replicate.common.RequestId;
import replicate.quorum.StoredValue;

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

package distrib.patterns.quorumconsensus.messages;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;
import distrib.patterns.quorumconsensus.StoredValue;

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

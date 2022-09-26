package distrib.patterns.quorum.messages;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

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

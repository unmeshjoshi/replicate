package replicate.quorumconsensus.messages;

import replicate.common.Request;
import replicate.common.RequestId;

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

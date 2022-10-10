package replicate.quorum.messages;

import replicate.common.Request;
import replicate.common.RequestId;

public class SetValueResponse extends Request {
    public final String result;

    public SetValueResponse(String result) {
        super(RequestId.SetValueResponse);
        this.result = result;
    }
}

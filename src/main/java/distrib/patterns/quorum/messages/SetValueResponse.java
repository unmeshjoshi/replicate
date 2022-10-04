package distrib.patterns.quorum.messages;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

public class SetValueResponse extends Request {
    public final String result;

    public SetValueResponse(String result) {
        super(RequestId.SetValueResponse);
        this.result = result;
    }
}

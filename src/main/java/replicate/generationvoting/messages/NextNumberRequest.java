package replicate.generationvoting.messages;

import replicate.common.Request;
import replicate.common.RequestId;

public class NextNumberRequest extends Request {
    public NextNumberRequest() {
        super(RequestId.NextNumberRequest);
    }
}

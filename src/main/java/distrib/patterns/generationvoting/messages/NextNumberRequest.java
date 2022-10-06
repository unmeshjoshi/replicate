package distrib.patterns.generationvoting.messages;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

public class NextNumberRequest extends Request {
    public NextNumberRequest() {
        super(RequestId.NextNumberRequest);
    }
}

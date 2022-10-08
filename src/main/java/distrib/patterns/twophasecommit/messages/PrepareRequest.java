package distrib.patterns.twophasecommit.messages;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

public class PrepareRequest extends Request {
    public PrepareRequest() {
        super(RequestId.Prepare);
    }
}

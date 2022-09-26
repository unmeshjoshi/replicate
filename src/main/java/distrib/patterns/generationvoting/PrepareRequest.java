package distrib.patterns.generationvoting;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

public class PrepareRequest extends Request {
    int number;

    public PrepareRequest(int number) {
        this();
        this.number = number;
    }

    public int getNumber() {
        return number;
    }
    //for jackson
    private PrepareRequest() {
        super(RequestId.PrepareRequest);
    }
}

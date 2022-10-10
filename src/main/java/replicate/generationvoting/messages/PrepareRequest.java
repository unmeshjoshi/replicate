package replicate.generationvoting.messages;

import replicate.common.Request;
import replicate.common.RequestId;

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
        super(RequestId.Prepare);
    }
}

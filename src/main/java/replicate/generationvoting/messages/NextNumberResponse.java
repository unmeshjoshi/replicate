package replicate.generationvoting.messages;

import replicate.common.Request;
import replicate.common.RequestId;

public class NextNumberResponse extends Request {
    int number;

    public NextNumberResponse(int number) {
        super(RequestId.NextNumberResponse);
        this.number = number;

    }

    public int getNumber() {
        return number;
    }

    //for jackson
    private NextNumberResponse() {
        super(RequestId.NextNumberResponse);
    }
}

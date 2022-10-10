package replicator.generationvoting.messages;

import replicator.common.Request;
import replicator.common.RequestId;

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

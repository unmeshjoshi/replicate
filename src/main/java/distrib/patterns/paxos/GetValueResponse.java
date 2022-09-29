package distrib.patterns.paxos;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

import java.util.Optional;

public class GetValueResponse extends Request {

    private Optional<String> value;

    public GetValueResponse(Optional<String> value) {
        this();
        this.value = value;
    }

    public Optional<String> getValue() {
        return value;
    }

    private GetValueResponse() {
        super(RequestId.GetValueResponse);
    }

}

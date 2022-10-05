package distrib.patterns.paxos;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

import java.util.Optional;

public class GetValueResponse extends Request {

    public final Optional<String> value;

    public GetValueResponse(Optional<String> value) {
        super(RequestId.GetValueResponse);
        this.value = value;
    }
}

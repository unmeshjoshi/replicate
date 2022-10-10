package replicator.paxos.messages;

import replicator.common.Request;
import replicator.common.RequestId;

import java.util.Optional;

public class GetValueResponse extends Request {

    public final Optional<String> value;

    public GetValueResponse(Optional<String> value) {
        super(RequestId.GetValueResponse);
        this.value = value;
    }
}

package replicate.paxos.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

import java.util.Optional;

public class GetValueResponse extends MessagePayload {

    public final Optional<String> value;

    public GetValueResponse(Optional<String> value) {
        super(MessageId.GetValueResponse);
        this.value = value;
    }
}

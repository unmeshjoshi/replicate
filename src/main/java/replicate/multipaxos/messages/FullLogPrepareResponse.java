package replicate.multipaxos.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;
import replicate.multipaxos.PaxosState;

import java.util.Map;


public class FullLogPrepareResponse extends MessagePayload {
    public final boolean promised;
    public final Map<Integer, PaxosState> uncommittedValues;

    public FullLogPrepareResponse(boolean promised, Map<Integer, PaxosState> uncommittedValues) {
        super(MessageId.Promise);
        this.promised = promised;
        this.uncommittedValues = uncommittedValues;
    }
}

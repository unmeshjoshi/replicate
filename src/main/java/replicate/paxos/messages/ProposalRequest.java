package replicate.paxos.messages;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class ProposalRequest extends MessagePayload {
    private MonotonicId monotonicId;
    private byte[] proposedValue;

    public ProposalRequest(MonotonicId monotonicId, byte[] proposedValue) {
        this();
        this.monotonicId = monotonicId;
        this.proposedValue = proposedValue;
    }

    public MonotonicId getMonotonicId() {
        return monotonicId;
    }

    public byte[] getProposedValue() {
        return proposedValue;
    }
    //for jackson
    private ProposalRequest() {
        super(MessageId.ProposeRequest);
    }
}

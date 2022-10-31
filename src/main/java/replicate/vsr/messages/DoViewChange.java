package replicate.vsr.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;
import replicate.vsr.ViewStampedReplication;

import java.util.Map;

public class DoViewChange extends MessagePayload {
    public final int viewNumber;
    public final Map<Integer, ViewStampedReplication.LogEntry> log;
    public final int normalStatusViewNumber;
    public final int opNumber;
    public final int commitNumber;

    public DoViewChange(int viewNumber, Map<Integer, ViewStampedReplication.LogEntry> log, int normalStatusViewNumber, int opNumber, int commitNumber) {
        super(MessageId.DoViewChange);
        this.viewNumber = viewNumber;
        this.log = log;
        this.normalStatusViewNumber = normalStatusViewNumber;
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
    }
}

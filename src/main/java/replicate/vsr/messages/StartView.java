package replicate.vsr.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;
import replicate.vsr.ViewStampedReplication;

import java.util.Map;

public class StartView extends MessagePayload {
    public final Map<Integer, ViewStampedReplication.LogEntry> log;
    public final int opNumber;
    public final int commitNumber;

    public StartView(Map<Integer, ViewStampedReplication.LogEntry> log, int opNumber, int commitNumber) {
        super(MessageId.StartView);

        this.log = log;
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
    }
}

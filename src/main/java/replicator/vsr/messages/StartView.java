package replicator.vsr.messages;

import replicator.common.Request;
import replicator.common.RequestId;
import replicator.vsr.ViewStampedReplication;

import java.util.Map;

public class StartView extends Request {
    public final Map<Integer, ViewStampedReplication.LogEntry> log;
    public final int opNumber;
    public final int commitNumber;

    public StartView(Map<Integer, ViewStampedReplication.LogEntry> log, int opNumber, int commitNumber) {
        super(RequestId.StartView);

        this.log = log;
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
    }
}

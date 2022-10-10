package replicate.wal;

import replicate.common.Config;

import java.util.ArrayList;
import java.util.List;

public class LogIndexBasedLogCleaner extends LogCleaner {
    private volatile Long snapshotIndex;

    public LogIndexBasedLogCleaner(Config config, WriteAheadLog wal, Long snapshotIndex) {
        super(config, wal);
        this.snapshotIndex = snapshotIndex;
    }

    public void updateShapshotIndex(Long snapshotIndex) {
        this.snapshotIndex = snapshotIndex;
    }

    //<codeFragment name="logIndexBasedLogCleaning"
    List<WALSegment> getSegmentsBefore(Long snapshotIndex) {
        List<WALSegment> markedForDeletion = new ArrayList<>();
        List<WALSegment> sortedSavedSegments = wal.sortedSavedSegments;
        for (WALSegment sortedSavedSegment : sortedSavedSegments) {
            if (sortedSavedSegment.getLastLogEntryIndex() < snapshotIndex) {
                markedForDeletion.add(sortedSavedSegment);
            }
        }
        return markedForDeletion;
    }
    //</codeFragment>

    @Override
    List<WALSegment> getSegmentsToBeDeleted() {
        return getSegmentsBefore(this.snapshotIndex);
    }
}

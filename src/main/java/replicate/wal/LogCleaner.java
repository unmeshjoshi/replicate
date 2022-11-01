package replicate.wal;

import replicate.common.Config;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class LogCleaner {
    final Config config;
    final WriteAheadLog wal;

    public LogCleaner(Config config, WriteAheadLog wal) {
        this.config = config;
        this.wal = wal;
    }

    //TODO:When multiple logs are created in multi-raft, a thread is created for each.Make this a shared threadpool instead
    private ScheduledExecutorService singleThreadedExecutor = Executors.newScheduledThreadPool(1);

    public void cleanLogs() {
        List<WALSegment> segmentsTobeDeleted = getSegmentsToBeDeleted();
        for (WALSegment walSegment : segmentsTobeDeleted) {
            wal.removeAndDeleteSegment(walSegment);
        }
        scheduleLogCleaning();
    }

    abstract List<WALSegment> getSegmentsToBeDeleted();

    //<codeFragment name="logCleanerStartup">
    public void startup() {
        scheduleLogCleaning();
    }

    private void scheduleLogCleaning() {
        singleThreadedExecutor.schedule(() -> {
            cleanLogs();
        }, config.getCleanTaskIntervalMs(), TimeUnit.MILLISECONDS);
    }
    //</codeFragment>
}

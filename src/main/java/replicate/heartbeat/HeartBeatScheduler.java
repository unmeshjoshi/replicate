package replicate.heartbeat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HeartBeatScheduler {
    private static final Logger logger = LogManager.getLogger(HeartBeatScheduler.class);
    private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

    private Runnable action;
    private Long heartBeatInterval;

    public HeartBeatScheduler(Runnable action, Long heartBeatIntervalMs) {
        this.action = action;
        this.heartBeatInterval = heartBeatIntervalMs;
    }

    private ScheduledFuture<?> scheduledTask;

    public void restart() {
        stop();
        start();
    }

    public void start() {
        if (scheduledTask != null && !scheduledTask.isCancelled()) {
            throw new IllegalStateException("ScheduledTask should be cancelled before starting the task again");
        }
        scheduledTask = executor.scheduleWithFixedDelay(new HeartBeatTask(action), heartBeatInterval, heartBeatInterval, TimeUnit.MILLISECONDS);
    }
    //</codeFragment>

    public void stop() {
        if (scheduledTask != null) {
            try {
                boolean cancelled = scheduledTask.cancel(true);
                logger.info("Stopped scheduled task " + cancelled);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class HeartBeatTask implements Runnable {
        private Runnable action;

        public HeartBeatTask(Runnable action) {
            this.action = action;
        }

        @Override
        public void run() {
            action.run();
        }
    }
}

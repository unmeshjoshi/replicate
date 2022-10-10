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

    public void start() {
        scheduledTask = executor.scheduleWithFixedDelay(new HeartBeatTask(action), heartBeatInterval, heartBeatInterval, TimeUnit.MILLISECONDS);
    }
    //</codeFragment>

    public void stop() {
        if (scheduledTask != null) {
            try {
                scheduledTask.cancel(true);
                logger.info("Stopped scheduler ");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void startWithRandomInterval() {
        var randomInterval = randomBetween(1000, 2000);
        scheduledTask = executor.scheduleWithFixedDelay(new HeartBeatTask(action), randomInterval, randomInterval, TimeUnit.MILLISECONDS);
    }

    private int randomBetween(int lowerBound, int upperBound) {
        return new Random().nextInt(upperBound - lowerBound) + lowerBound;
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

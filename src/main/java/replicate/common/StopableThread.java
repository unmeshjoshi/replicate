package replicate.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class StopableThread extends Thread {
    private static Logger logger = LogManager.getLogger(StopableThread.class.getName());

    private volatile boolean isRunning = true;

    @Override
    public void run() {
        try {
            while(isRunning) {
                doWork();
            }

        } catch(Exception e) {
            if (isRunning) {
                logger.error(e);
            }
        }
    }

    public void shutdown() {
        isRunning = false;
    }

    public abstract void doWork();
}

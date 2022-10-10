package replicate.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public interface Logging {
    default Logger getLogger() {
         var loggerName = this.getClass().getName();
         return LogManager.getLogger(loggerName);
    }
}

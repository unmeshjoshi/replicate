package replicate.net.requestwaitinglist;

import java.time.Duration;

public interface WaitingRequestCallback<T> extends RequestCallback<T>{
    boolean await(Duration duration);
}


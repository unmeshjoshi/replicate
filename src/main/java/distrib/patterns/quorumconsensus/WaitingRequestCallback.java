package distrib.patterns.quorumconsensus;

import com.google.common.util.concurrent.Uninterruptibles;
import distrib.patterns.common.RequestOrResponse;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

class WaitingRequestCallback implements distrib.patterns.net.requestwaitinglist.WaitingRequestCallback<RequestOrResponse> {
    CountDownLatch latch;

    public WaitingRequestCallback(int expectedResponses) {
        latch = new CountDownLatch(expectedResponses);
    }

    @Override
    public void onResponse(RequestOrResponse r) {
        System.out.println("Received Response = " + r);
        latch.countDown();
    }

    @Override
    public void onError(Throwable e) {
    }

    public boolean await(Duration duration) {
        return Uninterruptibles.awaitUninterruptibly(latch, duration);
    }
}

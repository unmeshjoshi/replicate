package distrib.patterns.leaderbasedpaxoslog;

import distrib.patterns.common.JsonSerDes;
import distrib.patterns.common.RequestOrResponse;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.requestwaitinglist.RequestCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class FullLogPrepareCallback implements RequestCallback<RequestOrResponse> {
    CountDownLatch latch = new CountDownLatch(3);
    List<FullLogPrepareResponse> promises = new ArrayList<>();

    @Override
    public void onResponse(RequestOrResponse r, InetAddressAndPort address) {
        promises.add(JsonSerDes.deserialize(r.getMessageBodyJson(), FullLogPrepareResponse.class));
        latch.countDown();
    }

    @Override
    public void onError(Exception e) {
    }

    public boolean isQuorumPrepared() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            //TODO
            e.printStackTrace();
        }
        return true;
    }
}

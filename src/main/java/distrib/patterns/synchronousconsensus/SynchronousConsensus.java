package distrib.patterns.synchronousconsensus;

import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.quorum.messages.SetValueRequest;
import distrib.patterns.quorum.messages.SetValueResponse;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SynchronousConsensus extends Replica {
    String value = "1";
    Set<String> values = new HashSet<>();

    public SynchronousConsensus(String name, Config config, SystemClock clock, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peerAddresses) throws IOException {
        super(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses);
    }

    @Override
    protected void registerHandlers() {
        handlesMessage(RequestId.SetValueRequest, this::setValue, SetValueRequest.class);


        handlesRequestBlocking(RequestId.SetValue, this::handleSetValueRequest, SetValueRequest.class)
                .respondsWith(RequestId.SetValueResponse, SetValueResponse.class);
        ;
    }

    private SetValueResponse handleSetValueRequest(SetValueRequest s) {
        int f = 2;
        for (int i = 1; i <= f + 1; i++) {
            values.add(s.getValue());
            sendOnewayMessageToOtherReplicas(s);

        }
        return new SetValueResponse(values.stream().findFirst().get());
    }

    public Void setValue(InetAddressAndPort from, SetValueRequest s) {
        if (!values.contains(s.getValue())) {
            values.add(s.getValue());
            sendOnewayMessageToOtherReplicas(s);

        }
        return null;
    }

}

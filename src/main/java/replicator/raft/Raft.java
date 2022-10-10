package replicator.raft;

import replicator.common.Config;
import replicator.common.Replica;
import replicator.common.SystemClock;
import replicator.net.InetAddressAndPort;

import java.io.IOException;
import java.util.List;

public class Raft extends Replica {
    private ServerRole serverRole = ServerRole.FOLLOWING;

    public Raft(String name, Config config, SystemClock clock, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peerAddresses) throws IOException {
        super(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses);
    }

    @Override
    protected void registerHandlers() {

    }
}

package distrib.patterns.vsr;

import distrib.patterns.net.InetAddressAndPort;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class Configuration {
    List<InetAddressAndPort> sortedReplicaAddresses;

    public Configuration(List<InetAddressAndPort> replicas) {
        List<InetAddressAndPort> lst = new ArrayList<>(replicas);
        Collections.sort(lst);
        sortedReplicaAddresses = Collections.unmodifiableList(lst);
    }

    public int replicaIndex(InetAddressAndPort address) {
        return sortedReplicaAddresses.indexOf(address);
    }

    public InetAddressAndPort getPrimaryForView(int viewNumber) {
        int primaryIndex = viewNumber % sortedReplicaAddresses.size();
        return sortedReplicaAddresses.get(primaryIndex);
    }
}

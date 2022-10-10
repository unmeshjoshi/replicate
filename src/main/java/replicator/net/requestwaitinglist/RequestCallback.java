package replicator.net.requestwaitinglist;

import replicator.net.InetAddressAndPort;

//<codeFragment name="requestCallback">
public interface RequestCallback<T> {
    void onResponse(T r, InetAddressAndPort fromNode);
    void onError(Exception e);
}
//</codeFragment>


package distrib.patterns.net.requestwaitinglist;

import distrib.patterns.net.InetAddressAndPort;

//<codeFragment name="requestCallback">
public interface RequestCallback<T> {
    void onResponse(T r, InetAddressAndPort fromNode);
    void onError(Exception e);
}
//</codeFragment>


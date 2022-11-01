package replicate.net.requestwaitinglist;

import replicate.net.InetAddressAndPort;

//<codeFragment name="requestCallback">
public interface RequestCallback<T> {
    void onResponse(T r, InetAddressAndPort fromNode);
    void onError(Exception e);
}
//</codeFragment>


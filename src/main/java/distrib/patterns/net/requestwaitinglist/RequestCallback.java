package distrib.patterns.net.requestwaitinglist;

//<codeFragment name="requestCallback">
public interface RequestCallback<T> {
    void onResponse(T r);
    void onError(Throwable e);
}
//</codeFragment>


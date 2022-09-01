package distrib.patterns.quorumconsensus;

public class GetVersionRequest {
    String key;

    public GetVersionRequest(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    //
    private GetVersionRequest() {
    }
}

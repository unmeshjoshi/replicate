package distrib.patterns.requests;

public class GetValueRequest {
    private String key;
    public GetValueRequest(String key) {
       this.key = key;
    }

    public String getKey() {
        return key;
    }

    //for jackson
    private GetValueRequest() {

    }
}

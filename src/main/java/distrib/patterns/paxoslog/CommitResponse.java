package distrib.patterns.paxoslog;

public class CommitResponse {
    boolean success;

    public CommitResponse(boolean success) {
        this.success = success;
    }

    //for jackson
    private CommitResponse() {
        
    }
}

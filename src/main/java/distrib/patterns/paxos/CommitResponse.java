package distrib.patterns.paxos;

public class CommitResponse {
    boolean success;

    public CommitResponse(boolean success) {
        this.success = success;
    }

    //for jackson
    private CommitResponse() {
        
    }
}

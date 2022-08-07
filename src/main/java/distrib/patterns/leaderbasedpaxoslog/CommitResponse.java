package distrib.patterns.leaderbasedpaxoslog;

public class CommitResponse {
    boolean success;

    public CommitResponse(boolean success) {
        this.success = success;
    }

    //for jackson
    private CommitResponse() {
        
    }
}

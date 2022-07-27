package distrib.patterns.paxos;

public class WriteTimeoutException extends RuntimeException {
    private int attempts;

    public WriteTimeoutException(int attempts) {
        this.attempts = attempts;
    }

}

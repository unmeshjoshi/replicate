package replicator.quorumconsensus;

public class ReadTimeoutException extends RuntimeException {
    public ReadTimeoutException(String s) {
        super(s);
    }
}

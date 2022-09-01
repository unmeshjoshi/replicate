package distrib.patterns.quorumconsensus;

public class ReadTimeoutException extends RuntimeException {
    public ReadTimeoutException(String s) {
        super(s);
    }
}

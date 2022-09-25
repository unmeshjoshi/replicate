package distrib.patterns.common;

import java.util.List;

public class QuorumCallException extends RuntimeException {
    List<Exception> exceptions;

    public QuorumCallException(List<Exception> exceptions) {
        this.exceptions = exceptions;
    }
}

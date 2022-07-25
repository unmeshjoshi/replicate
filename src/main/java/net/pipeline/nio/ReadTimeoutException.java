package net.pipeline.nio;

import java.io.IOException;

public class ReadTimeoutException extends IOException {
    public ReadTimeoutException(String s) {
        super(s);
    }
}

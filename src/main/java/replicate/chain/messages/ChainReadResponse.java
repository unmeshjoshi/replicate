package replicate.chain.messages;

import java.util.UUID;

public class ChainReadResponse {
    public final String key;
    public final String value;
    public final int version;
    public final UUID requestId;

    public ChainReadResponse(String key, String value, int version, UUID requestId) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.requestId = requestId;
    }
} 
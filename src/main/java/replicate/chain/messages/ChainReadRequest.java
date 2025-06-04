package replicate.chain.messages;

import java.util.UUID;

public class ChainReadRequest {
    public final String key;
    public final UUID requestId;

    public ChainReadRequest(String key, UUID requestId) {
        this.key = key;
        this.requestId = requestId;
    }
} 
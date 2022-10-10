package replicate.paxos.messages;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;

import java.util.Optional;

public class PrepareResponse extends Request {
    public final boolean promised;
    public final  Optional<String> acceptedValue;
    public final  Optional<MonotonicId> acceptedGeneration;

    public PrepareResponse(boolean success, Optional<String> acceptedValue, Optional<MonotonicId> acceptedGeneration) {
        super(RequestId.Promise);
        this.promised = success;
        this.acceptedValue = acceptedValue;
        this.acceptedGeneration = acceptedGeneration;
    }

    public PrepareResponse(boolean success) {
        this(success, Optional.empty(), Optional.empty());
    }

    @Override
    public String toString() {
        return "PrepareResponse{" +
                "promised=" + promised +
                ", acceptedValue=" + acceptedValue +
                ", acceptedGeneration=" + acceptedGeneration +
                '}';
    }
}

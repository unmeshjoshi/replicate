package replicate.paxoslog.messages;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;

import java.util.Optional;
public class PrepareResponse extends Request {
    public final boolean promised;
    public final Optional<byte[]> acceptedValue;
    public final Optional<MonotonicId> acceptedGeneration;

    public PrepareResponse(boolean success, Optional<byte[]> acceptedValue, Optional<MonotonicId> acceptedGeneration) {
        super(RequestId.Promise);
        this.promised = success;
        this.acceptedValue = acceptedValue;
        this.acceptedGeneration = acceptedGeneration;
    }

    @Override
    public String toString() {
        return "PrepareResponse{" +
                "promised=" + promised +
                ", acceptedValue=" + acceptedValue +
                ", acceptedBallot=" + acceptedGeneration +
                '}';
    }
}

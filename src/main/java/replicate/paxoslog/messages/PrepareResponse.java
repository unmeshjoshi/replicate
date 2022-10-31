package replicate.paxoslog.messages;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

import java.util.Optional;
public class PrepareResponse extends MessagePayload {
    public final boolean promised;
    public final Optional<byte[]> acceptedValue;
    public final Optional<MonotonicId> acceptedGeneration;

    public PrepareResponse(boolean success, Optional<byte[]> acceptedValue, Optional<MonotonicId> acceptedGeneration) {
        super(MessageId.Promise);
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

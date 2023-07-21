package replicate.paxos.messages;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

import java.util.Optional;

public class PrepareResponse extends MessagePayload {
    public final boolean promised;
    public final  Optional<byte[]> acceptedValue;
    public final  Optional<MonotonicId> acceptedBallot;

    public PrepareResponse(boolean success, Optional<byte[]> acceptedValue, Optional<MonotonicId> acceptedBallot) {
        super(MessageId.Promise);
        this.promised = success;
        this.acceptedValue = acceptedValue;
        this.acceptedBallot = acceptedBallot;
    }

    public PrepareResponse(boolean success) {
        this(success, Optional.empty(), Optional.empty());
    }

    @Override
    public String toString() {
        return "PrepareResponse{" +
                "promised=" + promised +
                ", acceptedValue=" + acceptedValue +
                ", acceptedBallot=" + acceptedBallot +
                '}';
    }
}

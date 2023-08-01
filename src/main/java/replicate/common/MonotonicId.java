package replicate.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

public class MonotonicId implements Comparable<MonotonicId> {
    public final int requestId;
    public final int serverId;

    public MonotonicId(int requestId, int serverId) {
        this.serverId = serverId;
        this.requestId = requestId;
    }

    public static MonotonicId empty() {
        return new MonotonicId(-1, -1);
    }

    public boolean isAfter(MonotonicId other) {
        if (this.requestId == other.requestId) {
            return this.serverId > other.serverId;
        }
        return this.requestId > other.requestId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MonotonicId that = (MonotonicId) o;
        return requestId == that.requestId && serverId == that.serverId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, serverId);
    }

    @Override
    public String toString() {
        return "MonotonicUUID{" +
                "id=" + requestId +
                ", serverId=" + serverId +
                '}';
    }

    public void serialize(DataOutputStream os) throws IOException {
        os.writeInt(requestId);
        os.writeInt(serverId);
    }

    public static MonotonicId deserialize(DataInputStream dataInputStream) throws IOException {
        return new MonotonicId(dataInputStream.readInt(), dataInputStream.readInt());
    }


    @Override
    public int compareTo(MonotonicId o) {
        int compare = Integer.compare(this.requestId, o.requestId);
        if (compare != 0) {
            return compare;
        }
        return Integer.compare(this.serverId, o.serverId);
    }

    public boolean isEmpty() {
        return this.equals(empty());
    }

    public boolean isBefore(MonotonicId title) {
        return this.compareTo(title) < 0;
    }

    public MonotonicId nextId(int serverId) {
        return new MonotonicId(requestId + 1, serverId);
    }
}

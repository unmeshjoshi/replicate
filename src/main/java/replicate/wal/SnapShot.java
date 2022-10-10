package replicate.wal;

import replicate.common.JsonSerDes;

import java.util.HashMap;

class SnapShot {
    private byte[] serializedKv;
    private Long lastAppliedEntryId;

    public SnapShot(byte[] serializedKv, Long lastAppliedEntryId) {
        this.serializedKv = serializedKv;
        this.lastAppliedEntryId = lastAppliedEntryId;
    }

    public byte[] getSerializedKv() {
        return serializedKv;
    }

    public Long getLastAppliedEntryId() {
        return lastAppliedEntryId;
    }

    HashMap deserializeState() {
        return JsonSerDes.deserialize(getSerializedKv(), HashMap.class);
    }
}

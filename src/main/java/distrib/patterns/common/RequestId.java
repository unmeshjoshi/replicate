package distrib.patterns.common;

import java.util.HashMap;
import java.util.Map;

public enum RequestId {
    VoteRequest(0),
    VoteResponse(1),
    HeartBeatRequest(2),
    HeartBeatResponse(3),
    ReplicationRequest(4),
    ReplicationResponse(5),
    StartElection(6),
    HeartbeatTick(7),
    ElectionTick(8),
    HandleVoteResponse(9),
    ProposeRequest(10),
    SetValueRequest(11),
    ConnectRequest(12),
    GetValueRequest(13),
    RedirectToLeader(14),
    HighWaterMarkTransmitted(15),
    WatchRequest(16),
    SetWatchRequest(17),
    LookingForLeader(18),
    RegisterLeaseRequest(19),
    RegisterClientRequest(20),
    ClientHeartbeat(21),
    PushPullGossipState(22),
    GossipVersions(23),
    RefreshLeaseRequest(24),
    GetPartitionTable(25),
    PartitionPutKV(26),
    PartitionGetKV(27),
    PartitionGetRangeKV(28),
    SetValueResponse(29),
    SetValueRequiringQuorum(30),
    BatchRequest(31),
    BatchResponse(32),
    GetValueResponse(33),
    PrepareRequest(34),
    Promise(35),
    Propose(36),
    Commit(37),
    ProposeResponse(38), CommitResponse(39);

    public static RequestId valueOf(Integer id) {
        return map.get(id);
    }

    int id;
    RequestId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    private static final Map<Integer, RequestId> map = new HashMap<>();
    static {
        for (RequestId pageType : RequestId.values()) {
            map.put(pageType.id, pageType);
        }
    }
}

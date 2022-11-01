package replicate.common;

import java.util.HashMap;
import java.util.Map;

public enum MessageId {
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
    Prepare(34),
    Promise(35),
    Propose(36),
    Commit(37),
    ProposeResponse(38),
    CommitResponse(39),
    FullPaxosLogPrepare(40),
    FullLogPrepareResponse(41),
    GetVersion(42),
    GetVersionResponse(43),
    NextNumberRequest(44),
    NextNumberResponse(45),
    VersionedSetValueRequest(46),
    VersionedGetValueRequest(47), ExcuteCommandRequest(48), ExcuteCommandResponse(49),
    SetValue(50),
    PrepareOK(51),
    PrepareNAK(52), StartViewChange(53), DoViewChange(54), StartView(55);

    public static MessageId valueOf(Integer id) {
        return map.get(id);
    }

    int id;
    MessageId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    private static final Map<Integer, MessageId> map = new HashMap<>();
    static {
        for (MessageId pageType : MessageId.values()) {
            map.put(pageType.id, pageType);
        }
    }
}

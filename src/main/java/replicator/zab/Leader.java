package replicator.zab;

import replicator.wal.Command;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class Leader extends Node {
    private Long id;
    long epoch = -1;
    private int quorum  = 2;
    List<Follower> followers = new ArrayList<>();

    private final AtomicLong hzxid = new AtomicLong(0);

    public void lead() {
        long epoch = getEpochToPropose(getId(), acceptedEpoch);
    }

    public void propose(Command command) {
        long zxid = nextZxid();
        LogEntry logEntry = new LogEntry(nextZxid(), command);
        log.put(zxid, logEntry);
        for (Follower follower : followers) {
            follower.propose(zxid, logEntry);
        }
    }

    private long nextZxid() {
        return hzxid.incrementAndGet();
    }

    private long getEpochToPropose(Long id, long lastAcceptedEpoch) {
        if (lastAcceptedEpoch >= epoch) {
            epoch = lastAcceptedEpoch + 1;
        }
        return epoch;
    }

    private Long getId() {
        return id;
    }

    List<Follower.FollowerInfo> followerInfos = new ArrayList<>();
    Long newEpoch;
    public void handleFollowerInfo(Follower.FollowerInfo followerInfo) {
        this.followerInfos.add(followerInfo);
        if (followerInfos.size() == quorum) {
            Long maxEpoch = followerInfos.stream().map(f -> f.acceptedEpoch).max(Long::compare).orElse(0L);
            newEpoch = maxEpoch + 1;
            hzxid.set(makeZxid(epoch, 0));
            sendNewEpochToFollowers(newEpoch);
        }
    }

    private void sendNewEpochToFollowers(Long newEpoch) {
        for (Follower follower : followers) {
            follower.handleNewEpoch(newEpoch);
        }
    }

    List<Long> newLeaderAck = new ArrayList<>();
    public void handleNewLeaderAck(long newEpoch) {
        newLeaderAck.add(newEpoch);
        if (newLeaderAck.size() == quorum) {
            sendCommit();
        }
    }

    private void sendCommit() {
        for (Follower follower : followers) {
            follower.handleCommit();
        }
    }

    private void sendCommit(long zxid) {
        for (Follower follower : followers) {
            follower.handleCommit(zxid);
        }
    }


    List<Long> acks = new ArrayList<>();
    public void handleAck(long zxid) {
        acks.add(zxid);
        if (acks.size() >= quorum) {
            sendCommit(zxid);
        }
    }

    static class FollowerEpochAck {
        long currentEpoch;
        Map<Long, LogEntry> log;
        long lastLoggedZxid;

        public FollowerEpochAck(long currentEpoch, Map<Long, LogEntry> log, long lastLoggedZxid) {
            this.currentEpoch = currentEpoch;
            this.log = log;
            this.lastLoggedZxid = lastLoggedZxid;
        }
    }

    List<FollowerEpochAck> epochAcks = new ArrayList<>();
    public void handleAckEpoch(FollowerEpochAck epochAck) {
        this.epochAcks.add(epochAck);
        if (epochAcks.size() == quorum) {
            Map<Long, LogEntry> log = selectLogWithMaxEpochOrMaxLogIndex(epochAcks);
            //phase 2
            sendNewLeader(newEpoch, log);
        }
    }

    static class NewLeaderInfo {
        long newEpoch;
        Map<Long, LogEntry> log;

        public NewLeaderInfo(Long newEpoch, Map<Long, LogEntry> log) {
            this.newEpoch = newEpoch;
            this.log = log;
        }
    }

    private void sendNewLeader(Long newEpoch, Map<Long, LogEntry> log) {
        for (Follower follower : followers) {
            follower.handleNewLeaderInfo(new NewLeaderInfo(newEpoch, log));
        }
    }

    private Map<Long, LogEntry> selectLogWithMaxEpochOrMaxLogIndex(List<FollowerEpochAck> epochAcks) {
        Optional<FollowerEpochAck> max = epochAcks.stream().max((e1, e2) -> {
            int compare = Long.compare(e1.currentEpoch, e2.currentEpoch);
            if (compare == 0) {
                return Long.compare(e1.lastLoggedZxid, e2.lastLoggedZxid);
            }
            return compare;
        });
        return max.get().log;
    }


}

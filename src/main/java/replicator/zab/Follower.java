package replicator.zab;

public class Follower extends Node {
    Leader potentialLeader;

    public Follower(Leader potentialLeader) {
        this.potentialLeader = potentialLeader;
    }

    public void handleNewEpoch(Long newEpoch) {
        if (newEpoch > acceptedEpoch) {
            acceptedEpoch = newEpoch;
            potentialLeader.handleAckEpoch(new Leader.FollowerEpochAck(currentEpoch, log, lastLoggedZxid));
        }
    }

    public void handleNewLeaderInfo(Leader.NewLeaderInfo newLeaderInfo) {
        this.currentEpoch = newLeaderInfo.newEpoch;
        this.log = newLeaderInfo.log;
        potentialLeader.handleNewLeaderAck(newLeaderInfo.newEpoch);
    }

    long commitIndex = 0;

    public void handleCommit(long aLong) {
        LogEntry logEntry = log.get(aLong);
        commit(logEntry);
        commitIndex = aLong;
    }

    public void handleCommit() {
        //commit all of the log;
        for (Long aLong : log.keySet()) {
            LogEntry logEntry = log.get(aLong);
            commit(logEntry);
        }
    }


    public void propose(long zxid, LogEntry logEntry) {
        log.put(zxid, logEntry);
        potentialLeader.handleAck(zxid);
    }

    static class FollowerInfo {
        long acceptedEpoch;

        public FollowerInfo(long acceptedEpoch) {
            this.acceptedEpoch = acceptedEpoch;
        }
    }

    public void registerWithLeader() {
        potentialLeader.handleFollowerInfo(new FollowerInfo(acceptedEpoch));
    }
}

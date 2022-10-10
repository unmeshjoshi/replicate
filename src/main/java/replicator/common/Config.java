package replicator.common;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Config {
    private String walDir;
    //TODO:Set sensible defaults. Max value was set to make sure logs are not cleaned during tests.
    private Long maxLogSize = Long.MAX_VALUE;
    private Long logMaxDurationMs = Long.MAX_VALUE;
    private List<Peer> servers;
    private Integer serverId;
    private Long electionTimeoutMs = 2000l;
    private long heartBeatIntervalMs = 100;
    private long followerTimeoutMs = 5000l;
    private boolean supportLogGroup = false;
    private boolean doAsyncRepair = false;

    public Config(String walDir) {
        this.walDir = walDir;
    }

    public Config withElectionTimeoutMs(Long electionTimeoutMs) {
        this.electionTimeoutMs = electionTimeoutMs;
        return this;
    }

    public Config withWalDir(String walDir) {
        this.walDir = walDir;
        return this;
    }


    public Config withHeartBeatIntervalMs(long heartBeatIntervalMs) {
        this.heartBeatIntervalMs = heartBeatIntervalMs;
        return this;
    }

    public Config withMaxLogSize(Long fileSize) {
        this.maxLogSize = fileSize;
        return this;
    }

    public Config(String walDir, Long maxLogSize) {
        this(walDir, maxLogSize, Collections.EMPTY_LIST);
    }

    public Config(String walDir, Long maxLogSize, List<Peer> servers) {
        this(0, walDir, maxLogSize, servers);
    }

    public Config(Integer serverId, String walDir, List<Peer> servers) {
        this(serverId, walDir, 1000l, servers);
    }
    public Config(Integer serverId, String walDir, Long maxLogSize, List<Peer> servers) {
        this(walDir);
        this.serverId = serverId;
        this.maxLogSize = maxLogSize;
        this.servers = servers;
    }

    public Long getLogMaxDurationMs() {
        return logMaxDurationMs;
    }

    public File getWalDir() {
        return new File(walDir);
    }

    public Long getMaxLogSize() {
        return maxLogSize;
    }

    public long getCleanTaskIntervalMs() {
        return 1000;
    }

    public List<Peer> getServers() {
        return servers;
    }

    public List<Peer> getPeers() {
        return servers.stream().filter((p) -> p.getId() != serverId).collect(Collectors.toList());
    }

    public int numberOfServers() {
        return getServers().size();
    }

    public int getNumberOfPeers() {
        return getPeers().size();
    }

    public Integer getServerId() {
        return serverId;
    }

    public Long getElectionTimeoutMs() {
        return electionTimeoutMs;
    }

    public int getHeartBeatIntervalMs() {
        return (int) heartBeatIntervalMs;
    }

    public Peer getServer(int leaderId) {
        List<Peer> servers = this.servers.stream().filter(p -> p.getId() == leaderId).collect(Collectors.toList());
        return servers.get(0);//
    }

    public long getFollowerWaitTimeoutMs() {
        return followerTimeoutMs;
    }

    public Config withFollowerTimeOutMs(int followerTimeoutMs) {
        this.followerTimeoutMs = followerTimeoutMs;
        return this;
    }

    public Config withLogMaxDurationMs(Long maxLogDuration) {
        this.logMaxDurationMs = maxLogDuration;
        return this;
    }

    public Config withGroupLog() {
        this.supportLogGroup = true;
        return this;
    }

    public boolean supportLogGroup() {
        return supportLogGroup;
    }


    public long getTransactionTimeoutMs() {
        return 2000;
    }

    public long getMaxBatchWaitTime() {
        return Duration.ofMillis(1).toNanos();

    }

    public void useAsyncReadRepair() {
        this.doAsyncRepair = true;
    }

    public boolean doAsyncReadRepair() {
        return doAsyncRepair;
    }

    public void setServerId(Integer serverId) {
        this.serverId = serverId;
    }
}

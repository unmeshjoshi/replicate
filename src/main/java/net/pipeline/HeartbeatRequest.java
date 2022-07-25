package net.pipeline;

public class HeartbeatRequest {
    private Integer serverId;

    //For Jaxon
    private HeartbeatRequest(){}

    public HeartbeatRequest(Integer serverId) {
        this.serverId = serverId;
    }

    public Integer getServerId() {
        return serverId;
    }
}

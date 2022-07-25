package net.pipeline.nio;

import common.RequestOrResponse;
import net.InetAddressAndPort;

public class SocketResponse {
    InetAddressAndPort address;
    RequestOrResponse response;

    public SocketResponse(InetAddressAndPort address, RequestOrResponse response) {
        this.address = address;
        this.response = response;
    }

    public InetAddressAndPort getAddress() {
        return address;
    }

    public RequestOrResponse getResponse() {
        return response;
    }
}

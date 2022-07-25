package net.pipeline.nio;

import common.RequestOrResponse;
import net.InetAddressAndPort;

class SocketRequestOrResponse {
    InetAddressAndPort address;
    RequestOrResponse request;

    public SocketRequestOrResponse(InetAddressAndPort address, RequestOrResponse request) {
        this.address = address;
        this.request = request;
    }

    public InetAddressAndPort getAddress() {
        return address;
    }

    public RequestOrResponse getRequest() {
        return request;
    }
}

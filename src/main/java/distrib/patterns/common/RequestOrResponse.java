package distrib.patterns.common;

import distrib.patterns.net.InetAddressAndPort;

import java.util.Arrays;
import java.util.Objects;

public class RequestOrResponse {
    private Integer requestId;
    private byte[] messageBodyJson;
    private Integer correlationId;
    private Integer generation = -1;
    InetAddressAndPort fromAddress;
    boolean isError;

    public RequestOrResponse setError() {
        isError = true;
        return this;
    }

    //for jackson
    private RequestOrResponse(){}

    //used by client
    public RequestOrResponse(Integer requestId, int correlationId) {
        this(requestId, "".getBytes(), correlationId);
    }

    //used by client and sending notifications
    public RequestOrResponse(Integer requestId, byte[] messageBodyJson) {
        this(requestId, messageBodyJson, -1);
    }

    //used by client and gossip
    public RequestOrResponse(Integer requestId, byte[] messageBodyJson, Integer correlationId) {
        this(-1, requestId, messageBodyJson, correlationId);
    }

    //by replicated log
    public RequestOrResponse(Integer generation, Integer requestId, byte[] messageBodyJson, Integer correlationId) {
        this(generation, requestId, messageBodyJson, correlationId, null);
    }

    public RequestOrResponse(Integer requestId, byte[] messageBodyJson, Integer correlationId, InetAddressAndPort fromAddress) {
        this(-1, requestId, messageBodyJson, correlationId, fromAddress);
    }

    public RequestOrResponse(int generation, Integer requestId, byte[] messageBodyJson, Integer correlationId, InetAddressAndPort fromAddress) {
        this.generation = generation;
        this.requestId = requestId;
        this.messageBodyJson = messageBodyJson;
        this.correlationId = correlationId;
        this.fromAddress = fromAddress;
    }

    public Integer getRequestId() {
        return requestId;
    }

    public byte[] getMessageBodyJson() {
        return messageBodyJson;
    }

    public Integer getCorrelationId() {
        return correlationId;
    }

    public Integer getGeneration() {
        return generation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestOrResponse that = (RequestOrResponse) o;
        return Objects.equals(requestId, that.requestId) && Arrays.equals(messageBodyJson, that.messageBodyJson) && Objects.equals(correlationId, that.correlationId) && Objects.equals(generation, that.generation) && Objects.equals(fromAddress, that.fromAddress);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(requestId, correlationId, generation, fromAddress);
        result = 31 * result + Arrays.hashCode(messageBodyJson);
        return result;
    }

    @Override
    public String toString() {
        return "RequestOrResponse{" +
                "requestId=" + requestId +
                ", messageBodyJson='" + messageBodyJson + '\'' +
                ", correlationId=" + correlationId +
                ", groupId=" + generation +
                '}';
    }

    public InetAddressAndPort getFromAddress() {
        return fromAddress;
    }

    public boolean isError() {
        return isError;
    }
}


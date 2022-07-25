package common;

import java.net.Socket;

public class Response {
    public static Response None = new Response();
    private RequestOrResponse response;
    private Socket to;

    private Response() {
    }

    public Response(RequestOrResponse response) {
        this.response = response;
    }
    public RequestOrResponse getResponse() {
        return response;
    }
    public boolean hasResponse() {
        return response != null;
    }
}

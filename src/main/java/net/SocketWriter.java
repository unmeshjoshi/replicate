package net;

import common.JsonSerDes;
import common.Message;
import common.RequestOrResponse;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.function.Function;

public class SocketWriter implements Function<Message<RequestOrResponse>, RequestOrResponse> {
    public RequestOrResponse apply(Message<RequestOrResponse> message) {
        message.getClientConnection().write(message.getRequest());
        return message.getRequest();
    }

    public void write(Socket socket, RequestOrResponse response) {
        try {
            var serializedMessage = JsonSerDes.serialize(response);
            var outputStream = socket.getOutputStream();
            var dataStream = new DataOutputStream(outputStream);
            var messageBytes = serializedMessage;
            dataStream.writeInt(messageBytes.length);
            dataStream.write(messageBytes);
            outputStream.flush();
        } catch (IOException e) {
            new NetworkException(e);
        }
    }
}


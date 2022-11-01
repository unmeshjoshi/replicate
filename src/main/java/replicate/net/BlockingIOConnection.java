package replicate.net;

import replicate.common.JsonSerDes;
import replicate.common.RequestOrResponse;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class BlockingIOConnection implements ClientConnection {
    private RequestConsumer server;
    Socket clientSocket;

    public BlockingIOConnection(RequestConsumer server, Socket clientSocket) {
        this.server = server;
        this.clientSocket = clientSocket;
    }

    //<codeFragment name="blockingClientConnectionWrite">
    @Override
    public synchronized void write(RequestOrResponse response) {
        try {
            var serializedMessage = JsonSerDes.serialize(response);
            var outputStream = clientSocket.getOutputStream();
            var dataStream = new DataOutputStream(outputStream);
            dataStream.writeInt(serializedMessage.length);
            dataStream.write(serializedMessage);
            dataStream.flush();

        } catch (Exception e) {
            e.printStackTrace();
            new NetworkException(e);
        }
    }
    //</codeFragment>

    public RequestOrResponse readRequest() {
        var responseBytes = readRequest(clientSocket);
        return deserialize(responseBytes);
    }

    private RequestOrResponse deserialize(byte[] responseBytes) {
        return JsonSerDes.deserialize(responseBytes, RequestOrResponse.class);
    }


    private byte[] readRequest(Socket socket) {
        try {
            var inputStream = socket.getInputStream();
            var dataInputStream = new DataInputStream(inputStream);
            var size = dataInputStream.readInt();
            var responseBytes = new byte[size];
            dataInputStream.read(responseBytes);
            return responseBytes;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            clientSocket.shutdownInput();
            clientSocket.shutdownOutput();
            clientSocket.close();
            server.close(this);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

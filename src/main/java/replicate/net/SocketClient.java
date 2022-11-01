package replicate.net;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replicate.common.JsonSerDes;
import replicate.common.RequestOrResponse;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class SocketClient<T> implements Closeable {
    private static Logger logger = LogManager.getLogger(SocketClient.class.getName());
    private final int readTimeoutMs = 5000;

    Socket clientSocket;

    public SocketClient(InetAddressAndPort address) throws IOException {
        this.clientSocket = new Socket(address.getAddress(), address.getPort());
        clientSocket.setSoTimeout(readTimeoutMs);
    }

    public void sendOneway(T message) {
        sendOneway(clientSocket, JsonSerDes.serialize(message));
    }

    public void sendOneway(Socket socket, byte[] serializedMessage) {
        try {
            var outputStream = socket.getOutputStream();
            var dataStream = new DataOutputStream(outputStream);
            var messageBytes = serializedMessage;
            dataStream.writeInt(messageBytes.length);
            dataStream.write(messageBytes);
            dataStream.flush();
        } catch (IOException e) {
            new RuntimeException(e);
        }
    }

    public byte[] read() {
      return read(clientSocket);
    }

    byte[] read(Socket socket) {
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

    public RequestOrResponse blockingSend(T requestOrResponse) {
        sendOneway(clientSocket, JsonSerDes.serialize(requestOrResponse));
        return JsonSerDes.deserialize(read(clientSocket), RequestOrResponse.class);
    }

    public void close() {
        try {
            clientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean isClosed() {
        return clientSocket.isClosed();
    }
}

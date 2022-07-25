package net;

import common.JsonSerDes;
import common.RequestOrResponse;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;


//<codeFragment name="ClientSocketClient">
public class SingleSocketChannel implements Closeable {
    final InetAddressAndPort address;
    final int heartbeatIntervalMs;
    private Socket clientSocket;
    private final OutputStream socketOutputStream;
    private final InputStream inputStream;

    public SingleSocketChannel(InetAddressAndPort address, int heartbeatIntervalMs) throws IOException {
        this.address = address;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        clientSocket = new Socket();
        clientSocket.connect(new InetSocketAddress(address.getAddress(), address.getPort()), heartbeatIntervalMs);
        clientSocket.setSoTimeout(heartbeatIntervalMs * 10); //set socket read timeout to be more than heartbeat.
        socketOutputStream = clientSocket.getOutputStream();
        inputStream = clientSocket.getInputStream();
    }

    public synchronized RequestOrResponse blockingSend(RequestOrResponse request) throws IOException {
        writeRequest(request);
        byte[] responseBytes = readResponse();
        return deserialize(responseBytes);
    }

    private void writeRequest(RequestOrResponse request) throws IOException {
        var dataStream = new DataOutputStream(socketOutputStream);
        byte[] messageBytes = serialize(request);
        dataStream.writeInt(messageBytes.length);
        dataStream.write(messageBytes);
    }
    //</codeFragment>

    //<codeFragment name="sendOneWay">
    public void sendOneWay(RequestOrResponse request) throws IOException {
        var dataStream = new DataOutputStream(socketOutputStream);
        byte[] messageBytes = serialize(request);
        dataStream.writeInt(messageBytes.length);
        dataStream.write(messageBytes);
    }
    //</codeFragment>

    private byte[] serialize(RequestOrResponse request) {
        return JsonSerDes.serialize(request);
    }

    //<codeFragment name="receive">
    public RequestOrResponse read() throws IOException {
        byte[] responseBytes = readResponse();
        return deserialize(responseBytes);
    }
    //</codeFragment>

    private byte[] readResponse() throws IOException {
        return read(inputStream);
    }

    byte[] read(InputStream inputStream) throws IOException {
        var dataInputStream = new DataInputStream(inputStream);
        var size = dataInputStream.readInt();
        var responseBytes = new byte[size];
        dataInputStream.read(responseBytes);
        return responseBytes;
    }

    private RequestOrResponse deserialize(byte[] responseBytes) {
        return JsonSerDes.deserialize(responseBytes, RequestOrResponse.class);
    }

    public void stop() {
        try {
            clientSocket.close();
        } catch (Exception e) {
            //ignore
        }
    }

    @Override
    public void close() throws IOException {
        stop();
    }

    public boolean isConnected() {
        return clientSocket != null
                && clientSocket.isConnected()
                && !clientSocket.isClosed();
    }

    public InetAddressAndPort getAddress() {
        return address;
    }
}

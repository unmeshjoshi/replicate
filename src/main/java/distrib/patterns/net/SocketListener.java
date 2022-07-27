package distrib.patterns.net;

import distrib.patterns.common.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SocketListener extends Thread implements Logging {
    private final InetAddressAndPort listenIp;
    private final Config config;
    private final ServerSocket serverSocket;
    private RequestConsumer server;
    private List<SocketHandlerThread> clientThreads = new ArrayList<>();

    public SocketListener(RequestConsumer server, InetAddressAndPort listenIp, Config config) {
        this.server = server;
        this.listenIp = listenIp;
        this.config = config;
        try {
            this.serverSocket = new ServerSocket();
            this.serverSocket.bind(new InetSocketAddress(listenIp.getAddress(), listenIp.getPort()));
            getLogger().info("Listening on " + listenIp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private AtomicBoolean running = new AtomicBoolean(true);

    //<codeFragment name="threadPerConnection">
    @Override
    public void run() {
        while (running.get()) {
            try {
                var clientSocket = serverSocket.accept();
                setReadTimeout(clientSocket);
                /**
                 * For a single connection, need to have a dedicated thread constantly reading from connection.
                 * Can be optimized by using NIO.
                 */
                SocketHandlerThread socketHandlerThread = new SocketHandlerThread(clientSocket);
                socketHandlerThread.start();
                clientThreads.add(socketHandlerThread);

            } catch (IOException e) {
                getLogger().debug(e);
            }
        }
    }
    //</codeFragment>

    //<codeFragment name="readTimeout">
    private void setReadTimeout(Socket clientSocket) throws SocketException {
        clientSocket.setSoTimeout(config.getHeartBeatIntervalMs() * 10);
    }
    //</codeFragment>


    public void shudown() {
        closeQuitely();
        closeAllClientConnections();
        running.set(false);
    }

    private void closeAllClientConnections() {
        for (SocketHandlerThread clientThread : clientThreads) {
            closeClient(clientThread);
        }
    }

    private void closeClient(SocketHandlerThread clientThread) {
        clientThread.closeConnection();
        clientThread.isRunning = false;
    }

    private void closeQuitely() {
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Boolean isBound() {
        return serverSocket.isBound();
    }

    public InetAddressAndPort getListenAddress() {
        return listenIp;
    }

    class SocketHandlerThread extends Thread implements Logging {
        private final BlockingIOConnection clientConnection;
        volatile boolean isRunning = false;

        public SocketHandlerThread(Socket clientSocket) {
            this.clientConnection = new BlockingIOConnection(server, clientSocket);
        }

        //<codeFragment name="ServerSocketHandlingThread">
        @Override
        public void run() {
            isRunning = true;
            try {
                //Continues to read/write to the socket connection till it is closed.
                while (isRunning) {
                    handleRequest();
                }
            } catch (Exception e) {
                getLogger().debug(e);
                closeClient(this);
            }
        }

        private void handleRequest() {
            RequestOrResponse request = clientConnection.readRequest();
            RequestId requestId = RequestId.valueOf(request.getRequestId());
            server.accept(new Message<>(request, requestId, clientConnection));
        }

        public void closeConnection() {
            clientConnection.close();
        }
        //</codeFragment>
    }
}

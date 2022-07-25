package net.pipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import common.Config;
import common.Message;
import common.RequestOrResponse;
import net.InetAddressAndPort;
import net.SocketListener;

public class Server {
    private static Logger logger = LogManager.getLogger(Server.class.getName());
    private Config config;
    private InetAddressAndPort listenAddress;
    private SocketListener listener;

    public Server(Config config, InetAddressAndPort listenAddress) {
        this.config = config;
        this.listenAddress = listenAddress;
        this.listener = new SocketListener(this::handleRequest, listenAddress, config);
    }

    private void handleRequest(Message<RequestOrResponse> request) {
        sendResponse(request);
    }

    public void start() {
        listener.start();
    }

    public void stop() {
        listener.shudown();
    }

    private void sendResponse(Message<RequestOrResponse> request) {
        logger.info("Sending response for = " + request.getRequest());
       request.getClientConnection().write(request.getRequest());
    }

    public Boolean isBound() {
        return listener.isBound();
    }
}

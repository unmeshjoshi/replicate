package net.nioserver.kafkastyle;

public class NioRequestHandler extends Thread {
    private RequestChannel channel;

    public NioRequestHandler(RequestChannel channel) {
        this.channel = channel;
    }

    @Override
    public void run() {
        while(true) {
            try {
                var req = channel.receiveRequest();
                handle(req);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void handle(RequestWrapper req) {
        channel.sendResponse(new ResponseWrapper(req.getProcessorId(), req.getKey(), req.getRequest()));
    }
}

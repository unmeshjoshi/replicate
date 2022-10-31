package replicate.common;

class Stage<Req extends MessagePayload> {
    Message<RequestOrResponse> message;
    Req request;

    public Stage(Message<RequestOrResponse> message, Req request) {
        this.message = message;
        this.request = request;
    }

    public Req getRequest() {
        return request;
    }

    public Message<RequestOrResponse> getMessage() {
        return message;
    }
}

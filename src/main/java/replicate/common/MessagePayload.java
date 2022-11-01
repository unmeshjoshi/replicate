package replicate.common;

public class MessagePayload {
    public final MessageId messageId;

    public MessagePayload(MessageId messageId) {
        this.messageId = messageId;
    }

    public MessageId getMessageId() {
        return messageId;
    }
}

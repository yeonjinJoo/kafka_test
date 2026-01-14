package kafka.monitor.util;

public abstract class NaiveMessageAdaptor implements IMessageAdaptor {

    private static final char DIV_CHAR = '!';

    protected final int messageSize;

    protected NaiveMessageAdaptor(int messageSize) {
        this.messageSize = messageSize;
    }

    abstract String getRandomPadding(int paddingSize);

    @Override
    public String generate(String messageId) {
        int paddingSize = messageSize - messageId.length() - 1;
        String padding = getRandomPadding(paddingSize);
        return messageId + DIV_CHAR + padding;
    }

    @Override
    public String extractMessageId(String message) {
        int messageIdSize = message.lastIndexOf(DIV_CHAR);
        if (messageIdSize < 0) {
            messageIdSize = message.length();
        }

        return message.substring(0, messageIdSize);
    }
}

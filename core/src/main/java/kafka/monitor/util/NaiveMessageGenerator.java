package kafka.monitor.util;

import java.util.Random;

public class NaiveMessageGenerator extends NaiveMessageAdaptor {

    private static final String PADDING_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private int[] preGeneratedIndices;

    private int curIdx;

    private static final Random RNG = new Random();

    public NaiveMessageGenerator(int messageSize, int preIndicesSize) {
        super(messageSize);
        init(preIndicesSize);
    }

    private void init(int preIndicesSize) {
        preGeneratedIndices = new int[preIndicesSize];
        curIdx = 0;

        for (int i = 0; i < preGeneratedIndices.length; i++) {
            preGeneratedIndices[i] = RNG.nextInt(PADDING_CHARACTERS.length());
        }
    }

    @Override
    protected String getRandomPadding(int size) {
        StringBuilder paddedString = new StringBuilder();
        for (int i = 0; i < size; i++) {
            if (curIdx >= preGeneratedIndices.length) {
                curIdx = 0;
            }
            char randomChar = PADDING_CHARACTERS.charAt(preGeneratedIndices[curIdx]);
            paddedString.append(randomChar);
            curIdx += 1;
        }

        return paddedString.toString();
    }
}

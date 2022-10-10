package replicator.wal;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class WALEntry {
//<codeFragment name="walEntry">
    private final Long entryIndex;
    private final byte[] data;
    private final EntryType entryType;
    private final long timeStamp;
//</codeFragment>
    private final Long generation;

    public WALEntry(Long entryIndex, byte[] data, EntryType entryType, long generation) {
        this.entryIndex = entryIndex;
        this.data = data;
        this.entryType = entryType;
        this.generation = generation;
        this.timeStamp = System.currentTimeMillis();
    }

    public Long getEntryIndex() {
        return entryIndex;
    }

    public byte[] getData() {
        return data;
    }

    public EntryType getEntryType() {
        return entryType;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public ByteBuffer serialize() {
        Integer entrySize = serializedSize();
        var bufferSize = logEntrySize(); //4 bytes for record length + walEntry size
        var buffer = ByteBuffer.allocate(bufferSize);
        buffer.clear();
        buffer.putInt(entrySize);
        buffer.putInt(entryType.getValue());
        buffer.putLong(generation);
        buffer.putLong(entryIndex);
        buffer.putLong(timeStamp);
        buffer.put(data);
        return buffer;
    }

    public Long getGeneration() {
        return generation;
    }

    Integer logEntrySize() { //4 bytes for size + size of serialized entry.
        return WriteAheadLog.sizeOfInt + serializedSize();
    }

    private Integer serializedSize() {
        return sizeOfData() + sizeOfIndex() + sizeOfGeneration() + sizeOfEntryType() + sizeOfTimestamp(); //size of all the fields
    }

    private int sizeOfData() {
        return data.length;
    }

    private int sizeOfEntryType() {
        return WriteAheadLog.sizeOfInt;
    }

    private int sizeOfTimestamp() {
        return WriteAheadLog.sizeOfLong;
    }

    private int sizeOfGeneration() {
        return WriteAheadLog.sizeOfLong;
    }

    private int sizeOfIndex() {
        return WriteAheadLog.sizeOfLong;
    }

    public boolean matchEntry(WALEntry entry) {
        return this.getGeneration() == entry.generation
                && this.entryIndex == entry.entryIndex
                && Arrays.equals(this.data, entry.data);
    }

    @Override
    public String toString() {
        return "WALEntry{" +
                "entryId=" + entryIndex +
                ", data=" + Arrays.toString(data) +
                ", entryType=" + entryType +
                ", timeStamp=" + timeStamp +
                ", generation=" + generation +
                '}';
    }
}

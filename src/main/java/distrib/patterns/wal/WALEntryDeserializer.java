package distrib.patterns.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

class WALEntryDeserializer {
    final ByteBuffer intBuffer = ByteBuffer.allocate(WriteAheadLog.sizeOfInt);
    final ByteBuffer longBuffer = ByteBuffer.allocate(WriteAheadLog.sizeOfLong);
    private FileChannel logChannel;

    public WALEntryDeserializer(FileChannel logChannel) {
        this.logChannel = logChannel;
    }

    WALEntry readEntry() {
        try {
            return readEntry(logChannel.position());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    class Header {
        long headerStartOffset;

        public Header(long headerStartOffset) {
            this.headerStartOffset = headerStartOffset;
        }

        Integer readEntryType() {
            return readInteger(headerStartOffset);
        }
        Long readGeneration() {
            return readLong(headerStartOffset + WriteAheadLog.sizeOfInt);
        }

        Long readEntryId() {
            return readLong(headerStartOffset + WriteAheadLog.sizeOfLong + WriteAheadLog.sizeOfInt);
        }

        Long readEntryTimestamp() {
            return readLong(headerStartOffset + WriteAheadLog.sizeOfLong + WriteAheadLog.sizeOfLong + WriteAheadLog.sizeOfInt);
        }

        public int getSize() {
            return WriteAheadLog.sizeOfInt + WriteAheadLog.sizeOfLong + WriteAheadLog.sizeOfLong + WriteAheadLog.sizeOfLong;
        }
    }

    WALEntry readEntry(long startPosition) {
        Integer entrySize = readInteger(startPosition);
        //read header
        Header header = new Header(startPosition + WriteAheadLog.sizeOfInt);
        Integer entryType = header.readEntryType();
        Long generation = header.readGeneration();
        Long entryId = header.readEntryId();
        Long entryTimestamp = header.readEntryTimestamp();

        int headerSize = header.getSize();
        var dataSize = (entrySize - headerSize);
        //read data
        ByteBuffer buffer = ByteBuffer.allocate(dataSize);
        var position = readFromChannel(logChannel, buffer, startPosition + headerSize + WriteAheadLog.sizeOfInt);
        var bytesRead = entrySize + WriteAheadLog.sizeOfInt;
        return new WALEntry(entryId, buffer.array(), EntryType.valueOf(entryType), generation);
    }

    public Long readLong(long position1) {
        long position = readFromChannel(logChannel, longBuffer, position1);
        return longBuffer.getLong();
    }

    public Integer readInteger(long position) {
        readFromChannel(logChannel, intBuffer, position);
        return intBuffer.getInt();
    }

    private long readFromChannel(FileChannel channel, ByteBuffer buffer, long filePosition) {

        try {
            buffer.clear();//clear to start reading.

            int bytesRead;
            do {
                bytesRead = channel.read(buffer, filePosition);
                filePosition += bytesRead;
            } while (bytesRead != -1 && buffer.hasRemaining());

            buffer.flip(); //read to be read

            return channel.position();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

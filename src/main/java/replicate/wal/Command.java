package replicate.wal;


import replicate.twophasecommit.CompareAndSwap;

import java.io.*;

public abstract class Command {
    public static final int NO_CLIENT_ID = -1;
    public static int SetValueType = 1;
    protected static final int CompositeCommandType = 16;
    protected static final int CasCommandType = 17;


    long clientId = -1;
    int requestNumber;

    public long getClientId() {
        return clientId;
    }

    public int getRequestNumber() {
        return requestNumber;
    }

    public <T extends Command> T withClientId(long clientId) {
        this.clientId = clientId;
        return (T) this;
    }

    public <T extends Command> T withRequestNumber(int requestNumber) {
        this.requestNumber = requestNumber;
        return (T) this;
    }

    public byte[] serialize() {
        try {
            var baos = new ByteArrayOutputStream();
            var dataInputStream = new DataOutputStream(baos);
            dataInputStream.writeLong(clientId);
            dataInputStream.writeInt(requestNumber);
            serialize(dataInputStream);
            return baos.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void serialize(DataOutputStream os) throws IOException;

    public static Command deserialize(InputStream is) {
      try {
          DataInputStream daos = new DataInputStream(is);
          var clientId = daos.readLong();
          var requestNumber = daos.readInt();
          int commandType = daos.readInt();
          if (commandType == SetValueType) {
              return SetValueCommand.deserialize(daos).withClientId(clientId).withRequestNumber(requestNumber);
          } else if (commandType == CompositeCommandType) {
              return CompositeCommand.deserialize(daos.readAllBytes());
          }
          else if (commandType == CasCommandType) {
            return CompareAndSwap.deserialize(daos.readAllBytes());

          } else throw new IllegalArgumentException("Unknown commandType " + commandType);
      } catch (Exception e) {
          throw new RuntimeException(e);
      }
    }

    public boolean hasClientId() {
        return clientId != NO_CLIENT_ID;
    }
}

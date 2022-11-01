package replicate.wal;

import com.google.common.base.Objects;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

public class TxnSetValueCommand extends Command {
//<codeFragment name = "setValueCommand">
    final String key;
    final String value;
    private final UUID txnId;

    public TxnSetValueCommand(UUID txnId, String key, String value) {
        this.key = key;
        this.value = value;
        this.txnId = txnId;
    }

    @Override
    public void serialize(DataOutputStream os) throws IOException {
        os.writeInt(Command.SetValueType);
        os.writeUTF(key);
        os.writeUTF(value);
        os.writeUTF(txnId.toString());
    }

    public static TxnSetValueCommand deserialize(InputStream is) {
        try {
            DataInputStream dataInputStream = new DataInputStream(is);
            return new TxnSetValueCommand(UUID.fromString(dataInputStream.readUTF()),
                    dataInputStream.readUTF(),
                    dataInputStream.readUTF());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
//</codeFragment>


    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    //for jaxon
    private TxnSetValueCommand() {
        key = "";
        value = "";
        txnId = null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnSetValueCommand that = (TxnSetValueCommand) o;
        return Objects.equal(key, that.key) && Objects.equal(value, that.value) && Objects.equal(txnId, that.txnId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key, value, txnId);
    }

    @Override
    public String toString() {
        return "TxnSetValueCommand{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", txnId=" + txnId +
                '}';
    }
}

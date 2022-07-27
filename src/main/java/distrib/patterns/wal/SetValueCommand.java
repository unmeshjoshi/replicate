package distrib.patterns.wal;

import com.google.common.base.Objects;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class SetValueCommand extends Command {
//<codeFragment name = "setValueCommand">
    final String key;
    final String value;
    final String attachLease;
    public SetValueCommand(String key, String value) {
        this(key, value, "");
    }
    public SetValueCommand(String key, String value, String attachLease) {
        this.key = key;
        this.value = value;
        this.attachLease = attachLease;
    }

    @Override
    public void serialize(DataOutputStream os) throws IOException {
        os.writeInt(Command.SetValueType);
        os.writeUTF(key);
        os.writeUTF(value);
        os.writeUTF(attachLease);
    }

    public static SetValueCommand deserialize(InputStream is) {
        try {
            DataInputStream dataInputStream = new DataInputStream(is);
            return new SetValueCommand(dataInputStream.readUTF(), dataInputStream.readUTF(), dataInputStream.readUTF());
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

    public boolean hasLease() {
        return !"".equals(attachLease) ;
    }

    public String getAttachedLease() {
        return attachLease;
    }

    //for jaxon
    private SetValueCommand() {
        key = "";
        value = "";
        attachLease = "";
    }

    @Override
    public String toString() {
        return "SetValueCommand{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", attachLease='" + attachLease + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SetValueCommand that = (SetValueCommand) o;
        return Objects.equal(key, that.key) && Objects.equal(value, that.value) && Objects.equal(attachLease, that.attachLease);
    }

    public boolean isEmpty() {
        return key.isEmpty() && value.isEmpty();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key, value, attachLease);
    }
}

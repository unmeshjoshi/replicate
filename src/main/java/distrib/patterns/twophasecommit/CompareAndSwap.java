package distrib.patterns.twophasecommit;

import distrib.patterns.common.JsonSerDes;
import distrib.patterns.wal.Command;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Optional;

public class CompareAndSwap extends Command {
    private String key;
    Optional<String> existingValue;
    String newValue;

    public CompareAndSwap(String key, Optional<String> existingValue, String newValue) {
        this.key = key;
        this.existingValue = existingValue;
        this.newValue = newValue;
    }

    public static Command deserialize(byte[] bytes) {
        return JsonSerDes.deserialize(bytes, CompareAndSwap.class);
    }

    public Optional<String> getExistingValue() {
        return existingValue;
    }

    public String getNewValue() {
        return newValue;
    }

    public String getKey() {
        return key;
    }

    @Override
    protected void serialize(DataOutputStream os) throws IOException {
        os.writeInt(Command.CasCommandType);
        os.write(JsonSerDes.serialize(this));
    }

    private CompareAndSwap() {
    }
}

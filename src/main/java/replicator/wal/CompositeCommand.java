package replicator.wal;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CompositeCommand extends Command {
    List<Command> commands = new ArrayList<>();
    @Override
    public void serialize(DataOutputStream os) throws IOException {
        os.writeInt(CompositeCommandType);
        os.writeInt(commands.size());
        for (Command command : commands) {
            byte[] bytes = command.serialize();
            Command deserialize = deserialize(new ByteArrayInputStream(bytes));

            os.writeInt(bytes.length);
            os.write(bytes);
        }
    }

    public static Command deserialize(byte[] bytes) {
        CompositeCommand cc = new CompositeCommand();
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        DataInputStream daos = new DataInputStream(is);
        try {
            int noOfCommands = daos.readInt();
            for (int i = 0; i < noOfCommands; i++) {
                int size = daos.readInt();
                byte[]data = new byte[size];
                daos.read(data);
                Command deserialize = deserialize(new ByteArrayInputStream(data));
                cc.add(deserialize);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return cc;
    }

    public void add(Command c) {
        commands.add(c);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompositeCommand that = (CompositeCommand) o;
        return Objects.equals(commands, that.commands);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commands);
    }

    public List<Command> getCommands() {
        return commands;
    }
}

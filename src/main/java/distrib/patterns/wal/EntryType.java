package distrib.patterns.wal;

import java.util.HashMap;
import java.util.Map;

public enum EntryType {
    DATA(0), METADATA(1), CRC(2);
    private int value;

    private static Map<Integer, EntryType> map = new HashMap<>();
    static {
        for (EntryType entryType : EntryType.values()) {
            map.put(entryType.value, entryType);
        }
    }
    public static EntryType valueOf(int entryType) {
        return (EntryType) map.get(entryType);
    }

    EntryType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}

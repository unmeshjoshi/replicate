package replicator.common;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class ClusterTest<T extends Replica> {
    protected Map<String, T> nodes = new HashMap<String, T>();
    @Before
    public void setUp() throws IOException {
    }
    @After
    public void tearDown() {
        nodes.values().stream().forEach(n -> n.shutdown());
    }
}

package common;

import distrib.patterns.common.Replica;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class ClusterTest<T extends Replica> {
    protected List<T> nodes = new ArrayList<>();
    @Before
    public void setUp() {
    }
    @After
    public void tearDown() {
        nodes.stream().forEach(n -> n.shutdown());
    }
}

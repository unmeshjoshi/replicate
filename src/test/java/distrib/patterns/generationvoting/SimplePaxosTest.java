package distrib.patterns.generationvoting;

import distrib.patterns.vsr.ClusterNode;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SimplePaxosTest {
    @Test
    public void clientChoosesGeneration() {
        ClusterNode athens = new ClusterNode();
        ClusterNode byzantium = new ClusterNode();
        ClusterNode cyrene = new ClusterNode();

        getRequestNumber(Arrays.asList(athens, byzantium, cyrene));
    }

    private void getRequestNumber(List<ClusterNode> asList) {

    }

}
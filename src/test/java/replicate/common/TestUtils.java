package replicate.common;

import replicate.net.InetAddressAndPort;
import replicate.net.Networks;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.Assert.fail;

public class TestUtils {
    private static Random random = new Random();
    public static File tempDir(String prefix) {
        var ioDir = System.getProperty("java.io.tmpdir");
        var f = new File(ioDir, prefix + random.nextInt(1000000));
        f.mkdirs();
        f.deleteOnExit();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Files.walkFileTree(f.toPath(), new SimpleFileVisitor< Path >() {
                    @Override
                    public FileVisitResult visitFileFailed(Path path, IOException exc) throws IOException {
                        // If the root path did not exist, ignore the error; otherwise throw it.
                        if (exc instanceof NoSuchFileException && path.toFile().equals(f))
                            return FileVisitResult.TERMINATE;
                        throw exc;
                    }

                    @Override
                    public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                        Files.delete(path);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path path, IOException exc) throws IOException {
                        if (exc != null) {
                            throw exc;
                        }

                        ;
                        List filesToKeep = new ArrayList<>();
                        if (!filesToKeep.contains(path.toFile())) {
                            Files.delete(path);
                        }

                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
        return f;
    }

    public static void waitUntilTrue(Supplier<Boolean> condition, String msg,
                                     Duration waitTime) {
        try {
            var startTime = System.nanoTime();
            while (true) {
                if (condition.get())
                    return;

                if (System.nanoTime() > (startTime + waitTime.toNanos())) {
                    fail(msg);
                }

                Thread.sleep(100L);
            }
        } catch (InterruptedException e) {
            // should never hit here
            throw new RuntimeException(e);
        }
    }

    //This seems to the right way to get random port for tests.
    //But Kafka had an issue https://issues.apache.org/jira/browse/KAFKA-1501
    //TODO: Figure out if there is any issue with this.
    public static int getRandomPort() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(0, 1000, new Networks().ipv4Address());
            serverSocket.setReuseAddress(false);
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {

                }
            }
        }
    }

    public static InetAddressAndPort randomAddress() {
        InetAddress inetAddress = new Networks().ipv4Address();
        return InetAddressAndPort.create(inetAddress.getHostAddress(), TestUtils.getRandomPort());
    }



    public static Config newConfig(int serverId, List<Peer> servers, long electionTimeoutMs, long heartBeatIntervalMs) {
        return new Config(serverId, tempDir(serverId), servers)
                .withElectionTimeoutMs(electionTimeoutMs)
                .withHeartBeatIntervalMs(heartBeatIntervalMs)
                .withMaxLogSize(Long.MAX_VALUE)
                .withLogMaxDurationMs(Long.MAX_VALUE);
    }

   public static String tempDir(int i) {
        return TestUtils.tempDir("test" + i).getAbsolutePath();
    }

    public static List<InetAddressAndPort> createNAddresses(int clusterSize) {
        List<InetAddressAndPort> addresses = new ArrayList<>();
        for (int i = 0; i < clusterSize; i++) {
            addresses.add(randomLocalAddress());
        }
        return addresses;
    }

    public static InetAddressAndPort randomLocalAddress() {
        var localhost = new Networks().ipv4Address().getHostAddress();
        return InetAddressAndPort.create(localhost, getRandomPort());
    }

    public interface ReplicaFactory<T extends Replica> {
        T create(String nodeName, Config config, SystemClock clock,
                 InetAddressAndPort clientConnectionAddress,
                 InetAddressAndPort peerConnectionAddress,
                 List<InetAddressAndPort> peerAddresses) throws IOException;
    }

    public static <T extends Replica> Map<String, T> startCluster(List<String> nodeNames, ReplicaFactory<T> factory) throws IOException {
        int clusterSize = nodeNames.size();
        Map<String, T> clusterNodes = new HashMap<>();
        SystemClock clock = new SystemClock();
        List<InetAddressAndPort> addresses = TestUtils.createNAddresses(clusterSize);
        List<InetAddressAndPort> clientInterfaceAddresses = TestUtils.createNAddresses(clusterSize);
        for (int i = 0; i < clusterSize; i++) {
            //public static void main(String[]args) {
            Config config = new Config(TestUtils.tempDir("clusternode_" + i).getAbsolutePath());
            config.setServerId(i);
            String nodeName = nodeNames.get(i);
            T replica =  factory.create(nodeName, config, clock, clientInterfaceAddresses.get(i), addresses.get(i), addresses);
            replica.start();

            //}
            clusterNodes.put(nodeName, replica);
        }
        return clusterNodes;
    }

}

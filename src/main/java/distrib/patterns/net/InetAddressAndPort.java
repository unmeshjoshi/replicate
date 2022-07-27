package distrib.patterns.net;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

public class InetAddressAndPort implements Comparable<InetAddressAndPort> {
    private InetAddress address;
    private Integer port;

    //for jaxon
    private InetAddressAndPort() {}

    public InetAddressAndPort(InetAddress address, Integer port) {
        this.address = address;
        this.port = port;
    }

    public static InetAddressAndPort create(String hostIp, Integer port)
    {
        try {
            return new InetAddressAndPort(InetAddress.getByName(hostIp), port);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public static InetAddressAndPort parse(String key) {
        var parts = key.substring(1, key.length() - 1).split(",");
        return InetAddressAndPort.create(parts[0], Integer.valueOf(parts[1]));
    }

    public InetAddress getAddress() {
        return address;
    }

    public Integer getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InetAddressAndPort that = (InetAddressAndPort) o;
        return Objects.equals(address, that.address) &&
                Objects.equals(port, that.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, port);
    }

    @Override
    public String toString() {
       return "[" +
                   address.getHostAddress() +
               "," +
                   port +
               ']';
    }

    @Override
    public int compareTo(InetAddressAndPort other) {
        int i = this.address.toString().compareTo(other.address.toString());
        return i == 0? Integer.compare(port, other.port) : i;
    }
}

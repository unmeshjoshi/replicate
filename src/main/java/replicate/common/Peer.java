package replicate.common;

import replicate.net.InetAddressAndPort;

public class Peer {
    private Integer id;
    private InetAddressAndPort address;
    private InetAddressAndPort clientListenAddress;

    public Peer(Integer id, InetAddressAndPort address, InetAddressAndPort clientListenAddress) {
        this.id = id;
        this.address = address;
        this.clientListenAddress = clientListenAddress;
    }

    public Integer getId() {
        return id;
    }

    public InetAddressAndPort getAddress() {
        return address;
    }

    public InetAddressAndPort getClientListenAddress() {
        return clientListenAddress;
    }

    @Override
    public String toString() {
        return "Peer{" +
                "id=" + id +
                ", address=" + address +
                '}';
    }
}

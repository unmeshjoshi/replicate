package replicate.net;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;
import java.util.stream.Collectors;


class NetworkInterfaceNotFound extends Exception {
    public NetworkInterfaceNotFound(String message) {
        super(message);
    }
}

public class Networks {
    public InetAddress ipv4Address() {
        try {
//            Map<Integer, List<InetAddress>> interfaceAddresses = NetworkInterfaceProvider.allInterfaces();
//            Set<Integer> sortedInterfaces = interfaceAddresses.keySet().stream().collect(Collectors.toCollection(LinkedHashSet::new));
//            List<InetAddress> inetAddresses = getInetAddresses(interfaceAddresses, sortedInterfaces);
//            InetAddress firstIpAddress = inetAddresses.get(0);
//            return firstIpAddress;

            return InetAddress.getLocalHost();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<InetAddress> getInetAddresses(Map<Integer, List<InetAddress>> interfaceAddresses, Set<Integer> sortedInterfaces) {
        Iterator<Integer> iterator = sortedInterfaces.iterator();
        Integer firstInterface = iterator.next();
        List<InetAddress> inetAddresses = interfaceAddresses.get(firstInterface);
        while(inetAddresses.size() == 0) {
            inetAddresses = interfaceAddresses.get(iterator.next());
        }
        return inetAddresses;
    }
}

class NetworkInterfaceProvider {

    private static Boolean isIpv4(InetAddress addr) {
        return !addr.isLoopbackAddress() && !(addr instanceof Inet6Address);
    }

    public static Map<Integer, List<InetAddress>> allInterfaces() throws SocketException {
        return NetworkInterface.networkInterfaces().map(iface -> {
            List<InetAddress> inetAddresses =
                    iface.inetAddresses().filter(inetAddress -> isIpv4(inetAddress))
                            .collect(Collectors.toList());
            return new HashMap.SimpleEntry<>(iface.getIndex(), inetAddresses);
        }).collect(Collectors.toUnmodifiableMap(HashMap.SimpleEntry::getKey, HashMap.SimpleEntry::getValue));
    }
}

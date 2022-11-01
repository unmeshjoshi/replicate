package replicate.net;

import replicate.common.Logging;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;

public class NIOSocketListener extends Thread implements Logging {
    private final ServerSocketChannel ss;
    private final Selector selector;
    private final RequestConsumer requestConsumer;

    public NIOSocketListener(RequestConsumer requestConsumer, InetAddressAndPort listenAddress) throws IOException {
        this.requestConsumer = requestConsumer;
        this.selector = Selector.open();
        this.ss = ServerSocketChannel.open();
        ss.socket().bind(new InetSocketAddress(listenAddress.getAddress(), listenAddress.getPort()));
        ss.configureBlocking(false);
        ss.register(selector, SelectionKey.OP_ACCEPT);
    }
    HashSet<NIOConnection> cnxns = new HashSet<NIOConnection>();
    @Override
    public void run() {
        while (!ss.socket().isClosed()) {
            try {
                selector.select(1000);
                Set<SelectionKey> selected;
                synchronized (this) {
                    selected = selector.selectedKeys();
                }
                ArrayList<SelectionKey> selectedList = new ArrayList<SelectionKey>(
                        selected);
                Collections.shuffle(selectedList);
                for (SelectionKey k : selectedList) {
                    if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                        SocketChannel sc = ((ServerSocketChannel) k
                                .channel()).accept();
                        sc.configureBlocking(false);
                        SelectionKey sk = sc.register(selector,
                                SelectionKey.OP_READ);
                        NIOConnection cnxn = createConnection(sc, sk);
                        sk.attach(cnxn);
                        addCnxn(cnxn);
                    } else if ((k.readyOps() & (SelectionKey.OP_READ | SelectionKey.OP_WRITE)) != 0) {
                        NIOConnection c = (NIOConnection) k.attachment();
                        c.doIO(k);
                    }
                }
                selected.clear();
            } catch (Exception e) {
                getLogger().error(e);
            }
        }
    }

    private void addCnxn(NIOConnection cnxn) {
        this.cnxns.add(cnxn);
    }

    protected NIOConnection createConnection(SocketChannel sock,
                                             SelectionKey sk) throws IOException {
        return new NIOConnection(sock, sk, this, requestConsumer);
    }



    public void shudown() {
         try {
            ss.close();
            clear();
            this.interrupt();
            this.join();
        } catch (InterruptedException e) {
            getLogger().warn("Interrupted",e);
        } catch (Exception e) {
            getLogger().error("Unexpected exception", e);
        }
    }

    synchronized public void clear() {
        selector.wakeup();
        synchronized (cnxns) {
            // got to clear all the connections that we have in the selector
            for (Iterator<NIOConnection> it = cnxns.iterator(); it
                    .hasNext();) {
                NIOConnection cnxn = it.next();
                it.remove();
                try {
                    cnxn.close();
                } catch (Exception e) {
                    // Do nothing.
                }
            }
        }

    }
}

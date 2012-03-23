package org.jgroups.protocols.pmcast.threading;

import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.pmcast.DeliverProtocol;
import org.jgroups.protocols.pmcast.manager.DeliverManager;

import java.util.List;

/**
 * The deliver threads. Is the only thread that delivers the Total Order Multicast message in order
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class DeliverThread extends Thread {
    private DeliverManager deliverManager;
    private boolean running = false;
    private DeliverProtocol groupMulticastProtocol;

    private final Log log = LogFactory.getLog(this.getClass());

    public DeliverThread(DeliverProtocol protocol) {
        super("Group-Multicast-Deliver-Thread");
        if (protocol == null) {
            throw new NullPointerException("Group Multicast Protocol can't be null");
        }
        this.groupMulticastProtocol = protocol;
    }

    public void start(DeliverManager deliverManager) {
        this.deliverManager = deliverManager;
        start();
    }

    @Override
    public void start() {
        if (deliverManager == null) {
            throw new NullPointerException("Deliver Manager can't be null");
        }
        running = true;
        super.start();
    }

    @Override
    public void run() {
        while (running) {
            try {
                List<Message> messages = deliverManager.getNextMessagesToDeliver();

                for (Message msg : messages) {
                    try {
                        groupMulticastProtocol.deliver(msg);
                    } catch(Throwable t) {
                        log.warn("Exception caught while delivering message " + msg + ":" + t.getMessage());
                    }
                }
            } catch (InterruptedException e) {
                //interrupted
            }
        }
    }

    @Override
    public void interrupt() {
        running = false;
        super.interrupt();
    }
}

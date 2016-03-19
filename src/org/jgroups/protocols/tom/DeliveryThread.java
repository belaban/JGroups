package org.jgroups.protocols.tom;

import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.List;

/**
 * The delivery thread. Is the only thread that delivers the Total Order Anycast message in order
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class DeliveryThread extends Thread {
    private DeliveryManager  deliveryManager;
    private volatile boolean running = false;
    private final DeliveryProtocol deliveryProtocol;

    private final Log log = LogFactory.getLog(this.getClass());

    public DeliveryThread(DeliveryProtocol protocol) {
        super("TOA-Delivery-Thread");
        if (protocol == null) {
            throw new NullPointerException("TOA Protocol can't be null");
        }
        this.deliveryProtocol= protocol;
    }

    public void start(DeliveryManager deliveryManager) {
        this.deliveryManager = deliveryManager;
        start();
    }

    public void setLocalAddress(String localAddress) {
        setName("TOA-Delivery-Thread-" + localAddress);
    }

    @Override
    public void start() {
        if (deliveryManager == null) {
            throw new NullPointerException("Delivery Manager can't be null");
        }
        running = true;
        super.start();
    }

    @Override
    public void run() {
        while (running) {
            try {
                List<Message> messages = deliveryManager.getNextMessagesToDeliver();

                for (Message msg : messages) {
                    try {
                        deliveryProtocol.deliver(msg);
                    } catch(Throwable t) {
                        log.warn("Exception caught while delivering message " + msg, t);
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

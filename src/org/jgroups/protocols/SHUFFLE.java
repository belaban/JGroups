package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;

import java.util.*;



/**
 * This layer shuffles upcoming messages, put it just above your bottom layer.
 * If you system sends less than 2 messages per sec you can notice a latency due
 * to this layer.
 *
 * @author Gianluca Collot
 *
 */

public class SHUFFLE extends Protocol implements Runnable {

    @Property
    String       name="SHUFFLE";
    final List         messages;
    Thread       messagesHandler;

    public SHUFFLE() {
        messages = Collections.synchronizedList(new ArrayList());
    }

    public String getName() {
        return name;
    }

    /**
     * Adds upcoming messages to the <code>messages List<\code> where the <code>messagesHandler<\code>
     * retrieves them.
     */

    public Object up(Event evt) {
        Message msg;

        switch (evt.getType()) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                // Do something with the event, e.g. extract the message and remove a header.
                // Optionally pass up
                messages.add(msg);
                return null;
        }

        return up_prot.up(evt);            // Pass up to the layer above us
    }




    /**
     * Starts the <code>messagesHandler<\code>
     */
    public void start() throws Exception {
        messagesHandler = new Thread(this,"MessagesHandler");
        messagesHandler.setDaemon(true);
        messagesHandler.start();
    }

    /**
     * Stops the messagesHandler
     */
    public void stop() {
        Thread tmp = messagesHandler;
        messagesHandler = null;
        try {
            tmp.join();
        } catch (Exception ex) {ex.printStackTrace();}
    }

    /**
     * Removes a random chosen message from the <code>messages List<\code> if there
     * are less than 10 messages in the List it waits some time to ensure to chose from
     * a set of messages > 1.
     */

    public void run() {
        Message msg;
        while (messagesHandler != null) {
            if (!messages.isEmpty()) {
                msg = (Message) messages.remove(rnd(messages.size()));
                up_prot.up(new Event(Event.MSG,msg));
            }
            if (messages.size() < 5) {
                try {
                    Thread.sleep(300); /** @todo make this time user configurable */
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }// while
        // PassUp remaining messages
        Iterator iter = messages.iterator();
        while (iter.hasNext()) {
            msg = (Message) iter.next();
            up_prot.up(new Event(Event.MSG,msg));
        }
    }

    // random integer between 0 and n-1
    int rnd(int n) { return (int)(Math.random()*n); }

}

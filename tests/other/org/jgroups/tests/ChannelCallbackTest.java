package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;

/**
 * @author Bela Ban
 * @version $Id: ChannelCallbackTest.java,v 1.2 2009/06/17 16:20:14 belaban Exp $
 */
public class ChannelCallbackTest extends ReceiverAdapter implements ChannelListener {
    JChannel channel;

    public static void main(String[] args) {
        try {
            new ChannelCallbackTest().start();
        }
        catch(ChannelException e) {
            e.printStackTrace();
        }
    }

    private void start() throws ChannelException {
        channel=new JChannel();
        channel.setReceiver(this);
        channel.addChannelListener(this);
        channel.connect("bla");
        channel.send(null, null, "hello world");
        Util.sleep(3000);
        channel.close();
    }

    public void receive(Message msg) {
        System.out.println("-- MSG: " + msg);
        try {
            Address dst=msg.getDest();
            if(dst == null || dst.isMulticastAddress())
                channel.send(msg.getSrc(), null, "this is a response");
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void viewAccepted(View new_view) {
        System.out.println("-- VIEW: " + new_view);
    }

    public void channelConnected(Channel channel) {
        System.out.println("-- channel connected: " + channel);
    }

    public void channelDisconnected(Channel channel) {
        System.out.println("-- channel disconnected: " + channel);
    }

    public void channelClosed(Channel channel) {
        System.out.println("-- channel closed: " + channel);
    }

    public void channelShunned() {
    }

    public void channelReconnected(Address addr) {
    }

}

// $Id: ChannelTest.java,v 1.3 2004/07/30 04:44:51 jiwils Exp $

package org.jgroups.tests;


import org.jgroups.*;


/**
 * Simple test - each instance broadcasts a message to the group and prints received messages to stdout.
 * Start one instance, then another one. Both instances will receive each other's messages.
 */
public class ChannelTest implements Runnable {
    private Channel channel=null;
    private Thread mythread=null;
    private boolean looping=true;


    public void start() throws Exception {
        channel=new JChannel();
        channel.connect("ExampleGroup");
        mythread=new Thread(this);
        mythread.start();
        for(int i=0; i < 30; i++) {
            System.out.println("Casting msg #" + i);
            channel.send(new Message(null, null, "Msg #" + i));
            Thread.sleep(1000);
        }
        channel.disconnect();
        channel.close();
        looping=false;
        mythread.interrupt();
        mythread.join(1000);
    }


    public void run() {
        Object obj;
        Message msg;
        while(looping) {
            try {
                obj=channel.receive(0); // no timeout
                if(obj instanceof View)
                    System.out.println("--> NEW VIEW: " + obj);
                else if(obj instanceof Message) {
                    msg=(Message)obj;
                    System.out.println("Received " + msg.getObject());
                }
            }
            catch(ChannelNotConnectedException conn) {
                break;
            }
            catch(Exception e) {
                System.err.println(e);
            }
        }
    }


    public static void main(String args[]) {
        ChannelTest test;
        try {
            test=new ChannelTest();
            test.start();
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }

}

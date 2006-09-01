package org.jgroups.demos;

import org.jgroups.Channel;
import org.jgroups.JChannelFactory;

/**
 * @author Bela Ban
 * @version $Id: DrawMultiplexer.java,v 1.2 2006/09/01 07:44:15 belaban Exp $
 */
public class DrawMultiplexer {
    JChannelFactory factory;

    public static void main(String[] args) throws Exception {
        String props="stacks.xml";
        for(int i=0; i < args.length; i++) {
            String arg=args[i];
            if(arg.equals("-props")) {
                props=args[++i];
                continue;
            }
            System.out.println("DrawMultiplexer [-help] [-props <stack config file>");
            return;
        }
        new DrawMultiplexer().start(props);
    }

    private void start(String props) throws Exception {
        factory=new JChannelFactory();
        factory.setMultiplexerConfig(props);

        final Channel ch1, ch2, ch3;
        ch1=factory.createMultiplexerChannel("udp", "id-1");
        ch1.connect("bela");

        ch2=factory.createMultiplexerChannel("udp", "id-2");
        ch2.connect("ban");

        // ch3=factory.createMultiplexerChannel("tcp", "TCP-based");
        // ch3.connect("bla");

        Thread t1=new Thread() {
            public void run() {
                try {
                    Draw draw1=new Draw(ch1);
                    draw1.go();
                }
                catch(Throwable t) {
                    t.printStackTrace();
                }
            }
        };



        Thread t2=new Thread() {
            public void run() {
                try {
                    Draw draw2=new Draw(ch2);
                    draw2.go();
                }
                catch(Throwable t) {
                    t.printStackTrace();
                }
            }
        };

        t1.start();
        t2.start();

        t1.join();
        t2.join();

    }
}

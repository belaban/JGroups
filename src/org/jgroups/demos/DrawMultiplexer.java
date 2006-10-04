package org.jgroups.demos;

import org.jgroups.Channel;
import org.jgroups.JChannelFactory;

/**
 * @author Bela Ban
 * @version $Id: DrawMultiplexer.java,v 1.4 2006/10/04 13:34:20 belaban Exp $
 */
public class DrawMultiplexer {
    JChannelFactory factory;

    public static void main(String[] args) throws Exception {
        String props="stacks.xml";
        String stack_name="udp";
        for(int i=0; i < args.length; i++) {
            String arg=args[i];
            if(arg.equals("-props")) {
                props=args[++i];
                continue;
            }
            if(arg.equals("-stack_name")) {
                stack_name=args[++i];
                continue;
            }
            System.out.println("DrawMultiplexer [-help] [-props <stack config file>] [-stack_name <name>]");
            return;
        }
        new DrawMultiplexer().start(props, stack_name);
    }

    private void start(String props, String stack_name) throws Exception {
        factory=new JChannelFactory();
        factory.setMultiplexerConfig(props);

        final Channel ch1, ch2, ch3;
        ch1=factory.createMultiplexerChannel(stack_name, "id-1");
        ch1.connect("bela");

        ch2=factory.createMultiplexerChannel(stack_name, "id-2");
        ch2.connect("ban");

        // ch3=factory.createMultiplexerChannel("tcp", "TCP-based");
        // ch3.connect("bla");

        Thread t1=new Thread() {
            public void run() {
                try {
                    Draw draw1=new Draw(ch1);
                    draw1.go();
                    draw1.stop();
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
                    draw2.stop();
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

        ch2.close();
        ch1.close();
        System.exit(0);
    }
}

package org.jgroups.demos;

import org.jgroups.Channel;
import org.jgroups.JChannelFactory;

/**
 * @author Bela Ban
 * @version $Id: DrawMultiplexer.java,v 1.8 2007/09/14 22:44:50 vlada Exp $
 */
public class DrawMultiplexer {
    JChannelFactory factory;

    public static void main(String[] args) throws Exception {
        String props="stacks.xml";
        String stack_name="udp";
        boolean state=false;
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
            if(arg.equals("-state")) {
                state=true;
                continue;
            }
            System.out.println("DrawMultiplexer [-help] [-props <stack config file>] [-stack_name <name>] [-state]");
            return;
        }
        new DrawMultiplexer().start(props, stack_name, state);
    }


    private void start(String props, String stack_name, boolean state) throws Exception {
        factory=new JChannelFactory();
        factory.setMultiplexerConfig(props);

        final Channel ch1, ch2;
        ch1=factory.createMultiplexerChannel(stack_name, "id-1");
        Draw draw1=new Draw(ch1, state, 5000);        

        ch2=factory.createMultiplexerChannel(stack_name, "id-2");
        Draw draw2=new Draw(ch2, state, 5000);        

        draw1.go();
        draw2.go();
    }
}

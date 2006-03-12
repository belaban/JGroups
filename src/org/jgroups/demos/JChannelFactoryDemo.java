package org.jgroups.demos;

import org.jgroups.Channel;
import org.jgroups.JChannelFactory;

/**
 * @author Bela Ban
 * @version $Id: JChannelFactoryDemo.java,v 1.1 2006/03/12 11:49:27 belaban Exp $
 */
public class JChannelFactoryDemo {
    JChannelFactory factory;

    public static void main(String[] args) throws Exception {
        String props="stacks.xml";
        for(int i=0; i < args.length; i++) {
            String arg=args[i];
            if(arg.equals("-props")) {
                props=args[++i];
                continue;
            }
            System.out.println("JChannelFactoryDemo [-help] [-props <stack config file>");
            return;
        }
        new JChannelFactoryDemo().start(props);
    }

    private void start(String props) throws Exception {
        factory=new JChannelFactory();
        factory.setMultiplexerConfig(props);

        Channel ch1, ch2;
        ch1=factory.createMultiplexerChannel("fc-fast-minimalthreads", "fast");
        ch2=factory.createMultiplexerChannel("tcp", "TCP-based");

        Draw draw1, draw2;
        draw1=new Draw(ch1);
        draw1.go();
 //       draw2=new Draw(ch2);
   //     draw2.go();
    }
}

package org.jgroups.demos.dynamic.commands;

import org.jgroups.JChannel;
import org.jgroups.demos.dynamic.Command;
import org.jgroups.util.Util;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Restarts the channel, with the same or a different configuration
 * @author Bela Ban
 * @since 3.1
 */
public class bounce extends Command {
    public Object invoke(Object[] args) throws Exception {
        if(args.length < 1)
            throw new IllegalArgumentException("need at least 1 arg (configuration)");
        Object config=args[0];
        JChannel ch=null;

        if(config instanceof byte[]) {
            InputStream input=new ByteArrayInputStream((byte[])config);
            ch=new JChannel(input);
        }
        else if(config instanceof String) {
            ch=new JChannel((String)config);
        }

        if(ch == null)
            return "couldn't initialize channel";

        final String cluster_name=test.getChannel().getClusterName();

        final JChannel tmp=ch;
        Thread thread=new Thread() {
            public void run() {
                Util.sleepRandom(2000, 5000);
                Util.close(test.getChannel());
                try {
                    tmp.connect(cluster_name);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
                test.setChannel(tmp);
                System.out.println("-- created new channel: " + tmp.getAddress());
            }
        };
        thread.start();

        return "about to create a new channel (in a few seconds)";
    }

    public String help() {
        return "bounce [<config file> | <byte buffer>]";
    }
}

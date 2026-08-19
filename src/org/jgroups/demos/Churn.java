package org.jgroups.demos;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.protocols.UFC;
import org.jgroups.util.Util;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Creates a cluster and then randomly has members leave and join for a given time. The view should be the same
 * on all of them after the test stops.
 * @author Bela Ban
 * @since x.y
 */
public class Churn {
    protected Entry[] channels;
    protected long    interval=100; // ms


    protected void start(int cluster_size, String cfg, long interval, double churn_rate, long time) throws Exception {
        channels=new Entry[cluster_size];
        for(int i=0; i < cluster_size; i++) {
            String name=String.valueOf((char)('A' + i));
            channels[i]=new Entry(new JChannel(cfg).name(name), name);
            channels[i].connect("churn");
        }

        int num_channels=Math.min(1, (int)(cluster_size * churn_rate));
        long target_time=System.currentTimeMillis() + time;

        while(System.currentTimeMillis() < target_time) {
            System.out.print(".");
            int picked=0;
            while(picked < num_channels) {
                int index=Util.random(channels.length) -1;
                if(channels[index].isConnected()) {
                    channels[index].disconnect();
                    picked++;
                }
            }

            for(Entry e: channels)
                if(!e.isConnected())
                    e.connect("churn");

            Util.sleep(interval);
        }

        System.out.printf("\n--- views --------\n%s\n",
                          Stream.of(channels).map(Entry::channel).map(ch -> String.format("%s: %s", ch.address(), ch.view()))
                            .collect(Collectors.joining("\n")));

        for(Entry e: channels) {
            JChannel ch=e.channel();
            UFC ufc=ch.stack().findProtocol(UFC.class);
            System.out.printf("%s: %s\n", ch.address(), print(ufc));
        }

        for(Entry e: channels)
            Util.close(e.channel());
    }

    protected static String print(UFC ufc) {
        Set<Address> receiver_keys=ufc.received().keySet(), sender_keys=ufc.sent().keySet();
        return String.format("recv: %s sent: %s", receiver_keys, sender_keys);
    }

    protected record Entry(JChannel channel, String name) {
        protected void connect(String cluster) throws Exception {
            channel.connect(cluster);
        }

        protected void    disconnect() {channel.disconnect();}
        protected boolean isConnected() {return channel.isConnected();}

    }

    public static void main(String[] args) throws Exception {
        long interval=100; // ms
        int cluster_size=10;
        double churn_rate=0.1; // 10%
        long time=60_000; // ms

        String cfg="/Users/bela/default-jgroups-tcp.xml";

        for(int i=0; i < args.length; i++) {
            if("-interval".equals(args[i])) {
                interval=Long.parseLong(args[++i]);
                continue;
            }
            if("-cfg".equals(args[i])) {
                cfg=args[++i];
                continue;
            }
            if("-churn_rate".equals(args[i])) {
                churn_rate=Double.parseDouble(args[++i]);
                continue;
            }
            if("-cluster_size".equals(args[i])) {
                cluster_size=Integer.parseInt(args[++i]);
                continue;
            }
            if("-time".equals(args[i])) {
                time=Long.parseLong(args[++i]);
                continue;
            }
            System.out.println("Churn [-cluster_size <size>] [-cfg config-file] [-interval ms] " +
                                 "[-churn_rate percentage] [-time ms]");
            return;
        }
        Churn ch=new Churn();
        ch.start(cluster_size, cfg, interval, churn_rate, time);
    }
}

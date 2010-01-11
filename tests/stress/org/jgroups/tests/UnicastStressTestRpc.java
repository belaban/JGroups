package org.jgroups.tests;

import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.util.Util;
import org.jgroups.blocks.MembershipListenerAdapter;
import org.jgroups.blocks.RpcDispatcher;

/**
 * Test mimicking the behavior of Inifinispan in DIST (sync and async) mode.
 * @author Bela Ban
 * @version $Id: UnicastStressTestRpc.java,v 1.1 2010/01/11 08:24:18 belaban Exp $
 */
public class UnicastStressTestRpc extends MembershipListenerAdapter {
    JChannel ch;
    RpcDispatcher disp;
    String props="udp.xml";
    int num_threads=1;
    int msg_size=1000; // bytes
    boolean async=true; // sync or async
    double writes=0.2; // 20% puts, 80% gets
    int num_owners=2;
    static final String CLUSTER_NAME="test-cluster";
    private String logical_name=null;


    public UnicastStressTestRpc(String props, int num_threads, int msg_size, boolean async, double writes,
                                int num_owners, String logical_name) {
        this.props=props;
        this.num_threads=num_threads;
        this.msg_size=msg_size;
        this.async=async;
        this.writes=writes;
        this.num_owners=num_owners;
        this.logical_name=logical_name;
    }


    private void start() throws ChannelException {
        ch=new JChannel(props);
        if(logical_name != null)
            ch.setName(logical_name);
        disp=new RpcDispatcher(ch, null, this, this);
        loop();
        ch.close();
    }


    public void viewAccepted(View new_view) {
        System.out.println("- new view: " + new_view);
    }


    private void loop() {
        int c;
        while(true) {
            c=Util.keyPress("[1] start [2] stop [q] quit");
            switch(c) {
                case '1':
                    break;
                case '2':
                    break;
                case 'q':
                    return;
            }
        }
    }



    public static void main(String[] args) throws ChannelException {
        String props="udp.xml";
        int num_threads=1;
        int msg_size=1000; // bytes
        boolean async=true; // sync or async
        double writes=0.2; // 20% puts, 80% gets
        int num_owners=2;
        String logical_name=null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-num_threads")) {
                num_threads=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-msg_size")) {
                msg_size=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-async")) {
                async=Boolean.valueOf(args[++i]);
                continue;
            }
            if(args[i].equals("-writes")) {
                writes=Double.parseDouble(args[++i]);
                continue;
            }
            if(args[i].equals("-num_owners")) {
                num_owners=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-name")) {
                logical_name=args[++i];
                continue;
            }
            help();
            return;
        }

        UnicastStressTestRpc test=new UnicastStressTestRpc(props, num_threads, msg_size, async, writes, num_owners, logical_name);
        test.start();
    }




    static void help() {
        System.out.println("UnicastStressTestRpc [-props config] [-num_threads <number of threads>] [-msg_size <bytes>] " +
                "[-async <true | false>] [-writes <percentage of writes>] [-name <logical name>]");
    }
}

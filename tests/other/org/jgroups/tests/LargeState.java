// $Id: LargeState.java,v 1.17 2006/03/27 08:06:44 belaban Exp $


package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.util.Util;


/**
 * Tests transfer of large states. Start first instance with -provider flag and -size flag (default = 1MB).
 * The start second instance without these flags: it should acquire the state from the first instance. Possibly
 * tracing should be turned on for FRAG to see the fragmentation taking place, e.g.:
 * <pre>
 * trace1=FRAG DEBUG STDOUT
 * </pre><br>
 * Note that because fragmentation might generate a lot of small fragments at basically the same time (e.g. size1MB,
 * FRAG.frag-size=4096 generates a lot of fragments), the send buffer of the unicast socket in UDP might be overloaded,
 * causing it to drop some packets (default size is 8096 bytes). Therefore the send (and receive) buffers for the unicast
 * socket have been increased (see ucast_send_buf_size and ucast_recv_buf_size below).<p>
 * If we didn't do this, we would have some retransmission, slowing the state transfer down.
 * 
 * @author Bela Ban Dec 13 2001
 */
public class LargeState extends ReceiverAdapter {
    Channel  channel;
    byte[]   state=null;
    Thread   getter=null;
    boolean  rc=false;
    String   props;
    long     start, stop;
    boolean  provider=true;


    public void start(boolean provider, int size, String props) throws Exception {
        this.provider=provider;
        channel=new JChannel(props);
        channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
        channel.setReceiver(this);
        channel.connect("TestChannel");
        System.out.println("-- connected to channel");

        if(provider) {
            System.out.println("Creating state of " + size + " bytes");
            state=createLargeState(size);
            System.out.println("Done. Waiting for other members to join and fetch large state");

//            System.out.println("sending a few messages");
//            for(int i=0; i < 100; i++) {
//                channel.send(null, null, "hello world " + i);
//            }
        }
        else {
            System.out.println("Getting state");
            start=System.currentTimeMillis();
            rc=channel.getState(null, 0);
            System.out.println("getState(), rc=" + rc);
        }

        // mainLoop();
        if(!provider) {
            channel.close();
        }
        else {
            for(;;) {
                Util.sleep(10000);
            }
        }
    }


    public void mainLoop() {
        Object ret;

        try {
            while(true) {
                ret=channel.receive(0);

                if(ret instanceof Message) {
                    System.out.println("-- received msg " + ((Message)ret).getObject() + " from " +
                            ((Message)ret).getSrc());
                }
                else if(ret instanceof GetStateEvent) {
                    System.out.println("--> returning state: " + ret);
                    channel.returnState(state);
                }
                else if(ret instanceof SetStateEvent) {
                    stop=System.currentTimeMillis();
                    byte[] new_state=((SetStateEvent)ret).getArg();
                    if(new_state != null) {
                        state=new_state;
                        System.out.println("<-- Received state, size = " + state.length +
                                " bytes (took " + (stop-start) + "ms)");
                    }
                    if(!provider)
                        break;
                }
            }
        }
        catch(Exception e) {
        }
    }


    byte[] createLargeState(int size) {
        return new byte[size];
    }

    public void receive(Message msg) {
        System.out.println("-- received msg " + msg.getObject() + " from " + msg.getSrc());
    }

    public void viewAccepted(View new_view) {
        if(provider)
            System.out.println("-- view: " + new_view);
    }

    public byte[] getState() {
        System.out.println("--> returning state: " + state.length + " bytes");
        return state;
    }

    public void setState(byte[] state) {
        stop=System.currentTimeMillis();
        if(state != null) {
            this.state=state;
            System.out.println("<-- Received state, size =" + state.length +
                    " (took " + (stop-start) + "ms)");
        }
    }



    public static void main(String[] args) {
        boolean provider=false;
        int size=1024 * 1024;
        String props="UDP(mcast_addr=239.255.0.35;mcast_port=7500;ip_ttl=2;" +
                "mcast_send_buf_size=150000;mcast_recv_buf_size=80000;" +
                "ucast_send_buf_size=80000;ucast_recv_buf_size=150000):" +
                "PING(timeout=2000;num_initial_members=3):" +
                "MERGE2(min_interval=5000;max_interval=10000):" +
                "FD_SOCK:" +
                "VERIFY_SUSPECT(timeout=1500):" +
                "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800):" +
                "UNICAST(timeout=300,600,900,1200,2400):" +
                "pbcast.STABLE(desired_avg_gossip=20000):" +
                "FRAG(frag_size=60000;down_thread=false;up_thread=false):" +
                "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
                "shun=false;print_local_addr=true):" +
                "pbcast.STATE_TRANSFER";



        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                help();
                return;
            }
            if("-provider".equals(args[i])) {
                provider=true;
                continue;
            }
            if("-size".equals(args[i])) {
                size=Integer.parseInt(args[++i]);
                continue;
            }
            if("-props".equals(args[i])) {
                props=args[++i];
            }
        }


        try {
            new LargeState().start(provider, size, props);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    static void help() {
        System.out.println("LargeState [-help] [-size <size of state in bytes] [-provider] [-props <properties>]");
    }

}

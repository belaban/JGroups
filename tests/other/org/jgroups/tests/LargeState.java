// $Id: LargeState.java,v 1.1 2003/09/09 01:24:13 belaban Exp $


package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.log.Trace;



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
 * @author Bela Ban Dec 13 2001
 */
public class LargeState {
    Channel        channel;
    byte[]         state=null;
    Thread         getter=null;
    boolean        rc=false;
    String         props;


    
    public void start(boolean provider, long size, String props) throws Exception {
	channel=new JChannel(props);
	channel.setOpt(Channel.GET_STATE_EVENTS, new Boolean(true));
	channel.connect("TestChannel");

	if(provider) {
	    System.out.println("Creating state of " + size + " bytes");
	    state=createLargeState(size);
	    System.out.println("Done. Waiting for other members to join and fetch large state");
	}
	else {
	    System.out.println("Getting state");
	    rc=channel.getState(null, 20000);
	    System.out.println("getState(), rc=" + rc);
	}

	mainLoop();
	
    }


    public void mainLoop() {
	Object ret;

	try {
	    while(true) {
		ret=channel.receive(0);

		if(ret instanceof Message) {
		}
		else if(ret instanceof GetStateEvent) {
		    System.out.println("--> returned state: " + ret);
		    channel.returnState(state);
		}
		else if(ret instanceof SetStateEvent) {
		    byte[] new_state=((SetStateEvent)ret).getArg();
		    if(new_state != null) {
			state=new_state;
			System.out.println("<-- Received state: size is " + state.length);
		    }
		}
	    }
	}
	catch(Exception e) {
	}
    }
    

    byte[] createLargeState(long size) {
	StringBuffer ret=new StringBuffer();
	for(int i=0; i < size; i++)
	    ret.append(".");
	return ret.toString().getBytes();
    }




    public static void main(String[] args) {
	boolean provider=false;
	long    size=1024 * 1024;
        String  props="UDP(mcast_addr=224.0.0.35;mcast_port=45566;ip_ttl=32;" +
                "mcast_send_buf_size=150000;mcast_recv_buf_size=80000;" +
                "ucast_send_buf_size=80000;ucast_recv_buf_size=150000):" +
                "PING(timeout=2000;num_initial_members=3):" +
                "MERGE2(min_interval=5000;max_interval=10000):" +
                "FD_SOCK:" +
                "VERIFY_SUSPECT(timeout=1500):" +
                "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800):" +
                "UNICAST(timeout=1000):" +
                "pbcast.STABLE(desired_avg_gossip=20000):" +
                "FRAG(frag_size=16000;down_thread=false;up_thread=false):" +
                "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
                "shun=false;print_local_addr=true):" +
                "pbcast.STATE_TRANSFER";

	Trace.init();
	
	for(int i=0; i < args.length; i++) {
	    if(args[i].equals("-help")) {
		help();
		return;
	    }
	    if(args[i].equals("-provider")) {
		provider=true;
		continue;
	    }
	    if(args[i].equals("-size")) {
		size=new Long(args[++i]).longValue();
		continue;
	    }
	    if(args[i].equals("-props")) {
		props=args[++i];
		continue;
	    }
	}


	try {
	    new LargeState().start(provider, size, props);
	}
	catch(Exception e) {
	    System.err.println(e);
	}
    }

    static void help() {
	System.out.println("LargeState [-help] [-size <size of state in bytes] [-provider] [-props <properties>]");
    }

}

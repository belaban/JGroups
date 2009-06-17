// $Id: GetStateTest.java,v 1.13 2009/06/17 16:28:59 belaban Exp $


package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.util.Util;




/**
 * Example of the STATE_TRANSFER protocol and API. The member periodically updates a shared state
 * (consisting of an array of length 3). New members join the group and fetch the current state before
 * they become operational. Existing members do not stop while new members fetch the group state.
 */
public class GetStateTest implements Runnable {
    Channel        channel;
    int[]          state;    // dice results, it *is* serializable !
    Thread         getter=null;
    boolean        rc=false;


    
    public void start() throws Exception {
	//String props="UDP:PING:FD:STABLE:NAKACK:UNICAST:FRAG:FLUSH:GMS:VIEW_ENFORCER:"+ 
	//  "STATE_TRANSFER:QUEUE";
	String props="UDP(mcast_addr=224.0.0.35;mcast_port=45566;ip_ttl=32;" +
            "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
            "PING(timeout=2000;num_initial_members=3):" +
            "MERGE2(min_interval=5000;max_interval=10000):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800):" +
            "UNICAST(timeout=5000):" +
            "pbcast.STABLE(desired_avg_gossip=20000):" +
            "FRAG(frag_size=4096;down_thread=false;up_thread=false):" +
            "pbcast.GMS(join_timeout=5000;" +
            "print_local_addr=true):" +
            "pbcast.STATE_TRANSFER";



	channel=new JChannel(props);
	channel.connect("TestChannel");

	System.out.println("Getting state");
	rc=channel.getState(null, 3000);
	System.out.println("getState(), rc=" + rc);
	if(rc == false) {
	    state=new int[3];
	    state[0]=1; state[1]=2; state[2]=3;
	}

	System.out.println("State is\n" + printState(state));
	Util.sleep(2000);

	getter=new Thread(this, "Getter");
	getter.start();


	while(true) {
	    Message update=new Message(null);
	    int     index=(int)  ((Math.random() * 10) % 3);
	    
	    try {
		update.setBuffer(Util.objectToByteBuffer(new Integer(index)));
	    }
	    catch(Exception e) {
		System.err.println(e);
	    }
	    System.out.println("Sending update for index " + index);
	    channel.send(update);
	    Util.sleep(2000);
	}
       
    }


    public void run() {
	Object ret;

	try {
	    while(true) {
		ret=channel.receive(0);

		if(ret instanceof Message) {
		    Message m=(Message)ret;
		    Integer index;
		    int     in;

		    try {
			index=(Integer)m.getObject();
			in=index.intValue();
			
			if(state != null) {
			    System.out.println("state[" + in + "]=" + (state[in]+1));
			    state[index.intValue()]++;
			}
		    }
		    catch(ClassCastException cast_ex) {
			System.out.println("Contents of buffer was no Integer !");
		    }
		    catch(Exception e) {
			// System.err.println(e);
		    }
		    
		}
		else if(ret instanceof GetStateEvent) {
		    System.out.println("----> State transfer: " + ret);
		    channel.returnState(Util.objectToByteBuffer(state));
		}
		else if(ret instanceof SetStateEvent) {
		    Object new_state=Util.objectFromByteBuffer(((SetStateEvent)ret).getArg());
		    if(new_state != null)
			state=(int[])new_state;
		}
	    }
	}
	catch(Exception e) {
	}
    }
    

    String printState(int[] vec) {
	StringBuilder ret=new StringBuilder();
	if(vec != null)
	    for(int i=0; i < vec.length; i++)
		ret.append("state[" + i + "]: " + vec[i] + '\n');
	return ret.toString();
    }


    public static void main(String[] args) {
	try {
	    new GetStateTest().start();
	}
	catch(Exception e) {
	    System.err.println(e);
	}
    }

}

package org.jgroups.tests;


import org.jgroups.blocks.DistributedHashtable;
import org.jgroups.log.Trace;
import org.jgroups.util.Util;
import org.jgroups.ChannelException;




/**
 * Title:        Java Groups Communications
 * Description:  Contact me at <a href="mailto:filip@filip.net">filip@filip.net</a>
 * Copyright:    Copyright (c) 2002
 * Company:      www.filip.net
 * @author Filip Hanik
 * @version 1.0
 */
public class DistributedHashDeadLock {

    public DistributedHashDeadLock() {
    }

    public static void main(String arg[]) throws ChannelException {
        System.out.println("Starting hashtable");
        String props="UDP(mcast_addr=224.0.0.35;mcast_port=45566;ip_ttl=32;mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
                "PING(timeout=2000;num_initial_members=3):" +
                "MERGE2:" +
                "FD:" +
                "VERIFY_SUSPECT(timeout=1500):" +
                "pbcast.NAKACK(max_xmit_size=16000;gc_lag=1500;retransmit_timeout=600,1200,2400,4800):" +
                "UNICAST(timeout=2000):" +
                "pbcast.STABLE(desired_avg_gossip=20000):" +
                "FRAG(frag_size=16000;down_thread=false;up_thread=false):" +
                "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=false;print_local_addr=true):" +
                "pbcast.STATE_TRANSFER";

        Trace.init();

        DistributedHashtable distHash=new DistributedHashtable("ADMINT", null, props, 20000);
        boolean odd=false;
        int numberofelements=1000;

        Util.sleep(5000);
        System.out.println("size: " + distHash.size());
        if(distHash.size() > 0)//The first instance counts up when numbers are even
        {                                        //The  second when numbers are odd
            odd=true;
        }
        boolean fillup=false;
        if(!odd) System.out.println("Loading hashtable with " + numberofelements + " elements. Don't start the other instance");
        for(int i=0; !odd && i < numberofelements; i++)//if the table isn't full we fill it
        {
            if(i % 50 == 0) System.out.print(i + " ");
            distHash.put("number" + i, new Integer(0));
            fillup=true;
        }
        if(fillup) System.out.println("\n\nHashtable filled, you can now start the other instance\n");
        System.out.println("initilising with odd: " + odd + " and size " + distHash.size());
        while(true) {
            try {
                System.out.println("#######################################################");
                Thread.sleep(10000);
                int count=0;
                for(int i=0; i < numberofelements; i++) {
                    int value=((Integer)distHash.get("number" + i)).intValue();
                    System.out.print(value + " ");
                    if(i % 50 == 0) System.out.print("\n");
                    if(odd && (value % 2 != 0))//If the number is odd, and the instance is supposed to count odd numbers up
                    {
                        count++;
                        value++;
                        distHash.put("number" + i, new Integer(value));
                        continue;
                    }
                    if(!odd && (value % 2 == 0))//If the number is even, and the instance is supposed to count even numbers up
                    {
                        count++;
                        value++;
                        distHash.put("number" + i, new Integer(value));
                        continue;
                    }
                }
                System.out.println("\n" + odd + " through all session, changed: " + count);

            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }

    }
}

package org.jgroups.demos;

import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.blocks.atomic.Counter;
import org.jgroups.blocks.atomic.CounterService;
import org.jgroups.util.Util;

public class CounterServiceDemo {

    static final String props="SHARED_LOOPBACK:PING(timeout=1000):" +
      "pbcast.NAKACK(use_mcast_xmit=false;log_discard_msgs=false;log_not_found_msgs=false)" +
      ":UNICAST:pbcast.STABLE(stability_delay=200):pbcast.GMS:FC:FRAG2:COUNTER";
    

    JChannel ch;

    void start(String props, String channel_name) throws Exception {



        ch=new JChannel(props);
        ch.setName(channel_name);
        ch.setReceiver(new ReceiverAdapter() {
            public void viewAccepted(View view) {
                System.out.println("-- view: " + view);
            }
        });
        CounterService counter_service=new CounterService(ch);
        ch.connect("counter-cluster");
        Counter counter=counter_service.getOrCreateCounter("mycounter", 1);

        boolean looping=true;
        while(looping) {
            try {
                int key=Util.keyPress("[1] Increment [2] Decrement [3] Create counter [4] Delete counter\n" +
                                      "[5] Print counters [8] Increment 1M times [x] Exit\n");
                switch(key) {
                    case '1':
                        long val=counter.incrementAndGet();
                        System.out.println("counter: " + val);
                        break;
                    case '2':
                        val=counter.decrementAndGet();
                        System.out.println("counter: " + val);
                        break;
                    case '3':
                        String counter_name=Util.readStringFromStdin("counter name: ");
                        counter=counter_service.getOrCreateCounter(counter_name, 1);
                        break;
                    case '4':
                        counter_name=Util.readStringFromStdin("counter name: ");
                        counter_service.deleteCounter(counter_name);
                        break;
                    case '5':
                        System.out.println("Counters (current=" + counter.getName() + "):\n\n" + counter_service.printCounters());
                        break;
                    case '8':
                        int NUM=Util.readIntFromStdin("num: ");
                        System.out.println("");
                        int print=NUM / 10;
                        long retval=0;
                        long start=System.currentTimeMillis();
                        for(int i=0; i < NUM; i++) {
                            retval=counter.incrementAndGet();
                            if(i > 0 && i % print == 0)
                                System.out.println("-- count=" + retval);
                        }
                        long diff=System.currentTimeMillis() - start;
                        System.out.println("\n" + NUM + " incrs took " + diff + " ms; " + (NUM / (diff / 1000.0)) + " ops /sec\n");
                        break;
                    case 'x':
                        looping=false;
                        break;
                }
            }
            catch(Throwable t) {
                System.err.println(t);
            }
        }
        Util.close(ch);
    }


    public static void main(final String[] args) throws Exception {
        String properties=props;
        String name=null;
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                properties=args[++i];
                continue;
            }
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            help();
            return;
        }


        new CounterServiceDemo().start(properties, name);

    }

    private static void help() {
        System.out.println("CounterServiceDemo [-props props] [-name name]");
    }


}

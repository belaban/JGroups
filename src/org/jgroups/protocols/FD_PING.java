package org.jgroups.protocols;

import org.apache.commons.logging.Log;
import org.jgroups.Message;
import org.jgroups.Event;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * @author Bela Ban
 * @version $Id: FD_PING.java,v 1.1 2006/06/01 10:51:04 belaban Exp $
 */
public class FD_PING extends FD {
    /** command to execute to ping host */
    String cmd="ping";

    public String getName() {
        return "FD_PING";
    }


    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);

        str=props.getProperty("cmd");
        if(str != null) {
            cmd=str;
            props.remove("cmd");
        }

        if(props.size() > 0) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }





    protected class PingMonitor extends Monitor {
         public void run() {
            Message hb_req;
            long not_heard_from; // time in msecs we haven't heard from ping_dest

            if(ping_dest == null) {
                if(warn)
                    log.warn("ping_dest is null: members=" + members + ", pingable_mbrs=" +
                            pingable_mbrs + ", local_addr=" + local_addr);
                return;
            }


            // 1. send heartbeat request
            hb_req=new Message(ping_dest, null, null);
            hb_req.putHeader(name, new FdHeader(FdHeader.HEARTBEAT));  // send heartbeat request
            if(log.isDebugEnabled())
                log.debug("sending are-you-alive msg to " + ping_dest + " (own address=" + local_addr + ')');
            passDown(new Event(Event.MSG, hb_req));
            num_heartbeats++;

            // 2. If the time of the last heartbeat is > timeout and max_tries heartbeat messages have not been
            //    received, then broadcast a SUSPECT message. Will be handled by coordinator, which may install
            //    a new view
            not_heard_from=System.currentTimeMillis() - last_ack;
            // quick & dirty fix: increase timeout by 500msecs to allow for latency (bela June 27 2003)
            if(not_heard_from > timeout + 500) { // no heartbeat ack for more than timeout msecs
                if(num_tries >= max_tries) {
                    if(log.isDebugEnabled())
                        log.debug("[" + local_addr + "]: received no heartbeat ack from " + ping_dest +
                                " for " + (num_tries +1) + " times (" + ((num_tries+1) * timeout) +
                                " milliseconds), suspecting it");
                    // broadcast a SUSPECT message to all members - loop until
                    // unsuspect or view change is received
                    bcast_task.addSuspectedMember(ping_dest);
                    num_tries=0;
                    if(stats) {
                        num_suspect_events++;
                        suspect_history.add(ping_dest);
                    }
                }
                else {
                    if(log.isDebugEnabled())
                        log.debug("heartbeat missing from " + ping_dest + " (number=" + num_tries + ')');
                    num_tries++;
                }
            }
        }
    }



    protected static class Pinger {

        static int execute(String command, Log log) throws IOException, InterruptedException {
            Process p=Runtime.getRuntime().exec(command);
            InputStream in=p.getInputStream(), err=p.getErrorStream();
            Reader in_reader, err_reader;
            in_reader=new Reader(in, log);
            err_reader=new Reader(err, log);
            in_reader.start();
            err_reader.start();
            in_reader.join();
            err_reader.join();
            return p.exitValue();
        }


        static class Reader extends Thread {
            InputStreamReader in;
            Log               log=null;
            boolean           trace=false;

            Reader(InputStream in, Log log) {
                this.in=new InputStreamReader(in);
                this.log=log;
                if(log != null) {
                    trace=log.isTraceEnabled();
                }
            }

            public void run() {
                int c;
                StringBuffer sb=new StringBuffer();
                while(true) {
                    try {
                        c=in.read();
                        if(c == -1)
                            break;
                        sb.append((char)c);
                    }
                    catch(IOException e) {
                        break;
                    }
                }
                if(trace)
                    log.trace(sb.toString());
            }
        }
    }

}

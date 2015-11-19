package org.jgroups.protocols;

import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.logging.Log;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;

/**
 * Protocol which uses an executable (e.g. /sbin/ping, or a script) to check whether a given host is up or not,
 * taking 1 argument; the host name of the host to be pinged. Property 'cmd' determines the program to be executed
 * (use a fully qualified name if the program is not on the path).
 * @author Bela Ban
 * @deprecated Use {@link org.jgroups.protocols.FD_HOST} instead. Will be removed in 4.0.
 */
@Unsupported
@Deprecated
public class FD_PING extends FD {
    /** Command (script or executable) to ping a host: a return value of 0 means success, anything else is a failure.
     * The only argument passed to cmd is the host's address (symbolic name or dotted-decimal IP address) */
    @Property(description="Command (script or executable) to ping a host: a return value of 0 means success, anything else is a failure. Default is ping")
    String cmd="ping";
  
    @Property(description="Write the stdout of the command to the log. Default is true")
    boolean verbose=true;

    protected Monitor createMonitor() {
        return new PingMonitor();
    }


    /**
     * Executes the ping command. Each time the command fails, we increment num_tries. If num_tries > max_tries, we
     * emit a SUSPECT message. If ping_dest changes, or we do receive traffic from ping_dest, we reset num_tries to 0.
     */
    protected class PingMonitor extends Monitor {

        public void run() {
            if(ping_dest == null) {
                if(log.isWarnEnabled())
                    log.warn("ping_dest is null: members=" + members + ", pingable_mbrs=" +
                            pingable_mbrs + ", local_addr=" + local_addr);
                return;
            }


            // 1. execute ping command
            String host=ping_dest instanceof IpAddress? ((IpAddress)ping_dest).getIpAddress().getHostAddress() : ping_dest.toString();
            String command=cmd + " " + host;
            if(log.isDebugEnabled())
                log.debug("executing \"" + command + "\" (own address=" + local_addr + ')');
            try {
                Log tmp_log=verbose? log : null;
                int rc=Pinger.execute(command, tmp_log);
                num_heartbeats++;
                if(rc == 0) { // success
                    num_tries.set(0);
                }
                else { // failure
                    num_tries.incrementAndGet();
                    if(log.isDebugEnabled())
                        log.debug("could not ping " + ping_dest + " (tries=" + num_tries + ')');
                }

                int tmp_tries=num_tries.get();
                if(tmp_tries >= max_tries) {
                    if(log.isDebugEnabled())
                        log.debug("[" + local_addr + "]: could not ping " + ping_dest + " for " + (tmp_tries +1) +
                                " times (" + ((tmp_tries+1) * timeout) + " milliseconds), suspecting it");
                    // broadcast a SUSPECT message to all members - loop until
                    // unsuspect or view change is received
                    bcast_task.addSuspectedMember(ping_dest);
                    num_tries.set(0);
                    if(stats) {
                        num_suspect_events++;
                        suspect_history.add(String.format("%s: %s", new Date(), ping_dest));
                    }
                }
            }
            catch(Exception ex) {
                if(log.isErrorEnabled())
                    log.error(Util.getMessage("FailedExecutingCommand") + command, ex);
            }
        }
    }



    protected static class Pinger {

        static int execute(String command, Log log) throws IOException, InterruptedException {
            Process p=Runtime.getRuntime().exec(command);
            InputStream in=p.getInputStream(), err=p.getErrorStream();
            try {
                Reader in_reader, err_reader;
                in_reader=new Reader(in, log);
                err_reader=new Reader(err, log);
                in_reader.start();
                err_reader.start();
                in_reader.join();
                err_reader.join();
                return p.exitValue();
            }
            finally {
                Util.close(in);
                Util.close(err);
            }
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
                StringBuilder sb=new StringBuilder();
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
                if(log.isTraceEnabled())
                    log.trace(sb.toString());
            }
        }
    }

}

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Failure detection protocol which detects the crash or hanging of entire hosts and suspects all cluster members
 * on those hosts. By default InetAddress.isReachable() is used, but any script/command can be used for liveness checks
 * by defining the 'cmd' property.
 * <p/>
 * FD_HOST does <em>not</em> detect the crash or hanging of single members on the local host, but only checks liveness
 * of all other hosts in a cluster. Therefore it is meant to be used together with other failure detection protocols,
 * e.g. {@link org.jgroups.protocols.FD_ALL} and {@link org.jgroups.protocols.FD_SOCK}.
 * <p/>
 * This protocol would typically be used when multiple cluster members are running on the same physical box.
 * <p/>
 * JIRA:  https://issues.jboss.org/browse/JGRP-1855
 * @author  Bela Ban
 * @version 3.5, 3.4.5
 */
@MBean(description="Failure detection protocol which detects crashes or hangs of entire hosts and suspects " +
  "all cluster members on those hosts")
public class FD_HOST extends Protocol {

    @Property(description="The command used to check a given host for liveness. Example: \"ping\". " +
      "If null, InetAddress.isReachable() will be used by default")
    protected String                                     cmd;

    @Property(description="Max time (in ms) after which a host is suspected if it failed all liveness checks")
    protected long                                       timeout=60000;

    @Property(description="The interval (in ms) at which the hosts are checked for liveness")
    protected long                                       interval=20000;

    @Property(description="Max time (in ms) that a liveness check for a single host can take")
    protected long                                       check_timeout=3000;

    @Property(description="Uses TimeService to get the current time rather than System.currentTimeMillis. Might get " +
      "removed soon, don't use !")
    protected boolean                                    use_time_service=true;

    @ManagedAttribute(description="Number of liveness checks")
    protected int                                        num_liveness_checks;

    @ManagedAttribute(description="Number of suspected events received")
    protected int                                        num_suspect_events;

    protected final Set<Address>                         suspected_mbrs=new HashSet<>();

    @ManagedAttribute(description="Shows whether there are currently any suspected members")
    protected volatile boolean                           has_suspected_mbrs;

    protected final BoundedList<Tuple<InetAddress,Long>> suspect_history=new BoundedList<>(20);

    protected Address                                    local_addr;
    protected InetAddress                                local_host;
    protected final List<Address>                        members=new ArrayList<>();

    /** The command to detect whether a target is alive */
    protected PingCommand                                ping_command=new IsReachablePingCommand();

    /** Map of hosts and their cluster members, updated on view changes. Used to suspect all members
     of a suspected host */
    protected final Map<InetAddress,List<Address>>       hosts=new HashMap<>();

    // Map of hosts and timestamps of last updates (ns)
    protected final ConcurrentMap<InetAddress, Long>     timestamps=new ConcurrentHashMap<>();

    /** Timer used to run the ping task on */
    protected TimeScheduler                              timer;

    protected TimeService                                time_service;

    protected Future<?>                                  ping_task_future;



    public FD_HOST pingCommand(PingCommand cmd) {this.ping_command=cmd; return this;}

    public void resetStats() {
        num_suspect_events=num_liveness_checks=0;
        suspect_history.clear();
    }

    public void setCommand(String command) {
        this.cmd=command;
        ping_command=this.cmd != null? new ExternalPingCommand(cmd) : new IsReachablePingCommand();
    }

    @ManagedOperation(description="Prints history of suspected hosts")
    public String printSuspectHistory() {
        StringBuilder sb=new StringBuilder();
        for(Tuple<InetAddress,Long> tmp: suspect_history) {
            sb.append(new Date(tmp.getVal2())).append(": ").append(tmp.getVal1()).append("\n");
        }
        return sb.toString();
    }

    @ManagedOperation(description="Prints timestamps")
    public String printTimestamps() {
        return _printTimestamps();
    }

    @ManagedAttribute(description="Whether the ping task is running")
    public boolean isPingerRunning() {
        Future<?> future=ping_task_future;
        return future != null && !future.isDone();
    }

    @ManagedOperation(description="Prints the hosts and their associated cluster members")
    public String printHosts() {
        StringBuilder sb=new StringBuilder();
        synchronized(hosts) {
            for(Map.Entry<InetAddress,List<Address>> entry: hosts.entrySet()) {
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
        }
        return sb.toString();
    }

    @ManagedOperation(description="Checks whether the given host is alive")
    public boolean isAlive(String host) throws Exception {
        return ping_command != null && ping_command.isAlive(InetAddress.getByName(host), check_timeout);
    }

    @ManagedAttribute(description="Currently suspected members")
    public String getSuspectedMembers() {return suspected_mbrs.toString();}

    public void init() throws Exception {
        if(interval >= timeout)
            throw new IllegalArgumentException("interval (" + interval + ") has to be less than timeout (" + timeout + ")");
        super.init();
        if(cmd != null)
            ping_command=new ExternalPingCommand(cmd);
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer not set");
        time_service=getTransport().getTimeService();
        if(time_service == null)
            log.warn("%s: time service is not available, using System.currentTimeMillis() instead", local_addr);
        else {
            if(time_service.interval() > timeout) {
                log.warn("%s: interval of time service (%d) is greater than timeout (%d), disabling time service",
                         local_addr, time_service.interval(), timeout);
                use_time_service=false;
            }
        }
        suspected_mbrs.clear();
        has_suspected_mbrs=false;
    }

    public void stop() {
        super.stop();
        stopPingerTask();
        suspected_mbrs.clear();
        has_suspected_mbrs=false;
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                View view=(View)evt.getArg();
                handleView(view);
                break;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
            case Event.CONNECT:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                local_host=getHostFor(local_addr);
                break;
            case Event.DISCONNECT:
                Object retval=down_prot.down(evt);
                local_host=null;
                return retval;

            case Event.UNSUSPECT:
                Address mbr=(Address)evt.getArg();
                unsuspect(mbr);
                break;
        }
        return down_prot.down(evt);
    }


    protected void handleView(View view) {
        List<Address> view_mbrs=view.getMembers();
        boolean is_pinger=false;
        members.clear();
        members.addAll(view_mbrs);
        Collection<InetAddress> current_hosts=null;
        synchronized(hosts) {
            hosts.clear();
            for(Address mbr: view_mbrs) {
                InetAddress key=getHostFor(mbr);
                if(key == null)
                    continue;
                List<Address> mbrs=hosts.get(key);
                if(mbrs == null)
                    hosts.put(key, mbrs=new ArrayList<>());
                mbrs.add(mbr);
            }
            is_pinger=isPinger(local_addr);
            current_hosts=new ArrayList<>(hosts.keySet());
        }

        if(suspected_mbrs.retainAll(view.getMembers()))
            has_suspected_mbrs=!suspected_mbrs.isEmpty();

        timestamps.keySet().retainAll(current_hosts);
        current_hosts.remove(local_host);
        for(InetAddress host: current_hosts)
            timestamps.putIfAbsent(host, getTimestamp());

        if(is_pinger)
            startPingerTask();
        else {
            stopPingerTask();
            timestamps.clear();
        }
    }


    protected PhysicalAddress getPhysicalAddress(Address logical_addr) {
        return (PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, logical_addr));
    }

    protected InetAddress getHostFor(Address mbr) {
        PhysicalAddress phys_addr=getPhysicalAddress(mbr);
        return phys_addr instanceof IpAddress? ((IpAddress)phys_addr).getIpAddress() : null;
    }

    protected boolean isPinger(Address mbr) {
        InetAddress host=getHostFor(mbr);
        if(host == null) return false; // should not happen
        List<Address> mbrs=hosts.get(host);
        return mbrs != null && !mbrs.isEmpty() && mbrs.get(0).equals(mbr);
    }

    protected void startPingerTask() {
        if(ping_task_future == null || ping_task_future.isDone())
            ping_task_future=timer.scheduleAtFixedRate(new PingTask(), interval, interval, TimeUnit.MILLISECONDS);
    }

    protected void stopPingerTask() {
        if(ping_task_future != null) {
            ping_task_future.cancel(false);
            ping_task_future=null;
        }
    }

    /** Called by ping task; will result in all members of host getting suspected */
    protected void suspect(InetAddress host) {
        List<Address> suspects;
        suspect_history.add(new Tuple<>(host, System.currentTimeMillis())); // we need wall clock time here
        synchronized(hosts) {
            List<Address> tmp=hosts.get(host);
            suspects=tmp != null? new ArrayList<>(tmp) : null;
        }
        if(suspects != null) {
            log.debug("%s: suspecting host %s; suspected members: %s", local_addr, host, Util.printListWithDelimiter(suspects, ","));
            suspect(suspects);
        }
    }


    protected void suspect(List<Address> suspects) {
        if(suspects == null || suspects.isEmpty())
            return;

        num_suspect_events+=suspects.size();

        final List<Address> eligible_mbrs=new ArrayList<>();
        synchronized(this) {
            suspected_mbrs.addAll(suspects);
            eligible_mbrs.addAll(members);
            eligible_mbrs.removeAll(suspected_mbrs);
            has_suspected_mbrs=!suspected_mbrs.isEmpty();
        }

        // Check if we're coord, then send up the stack
        if(local_addr != null && !eligible_mbrs.isEmpty()) {
            Address first=eligible_mbrs.get(0);
            if(local_addr.equals(first)) {
                log.debug("%s: suspecting %s", local_addr, suspected_mbrs);
                for(Address suspect: suspects) {
                    up_prot.up(new Event(Event.SUSPECT, suspect));
                    down_prot.down(new Event(Event.SUSPECT, suspect));
                }
            }
        }
    }

   /* protected void unsuspect(InetAddress host) {
        List<Address> suspects;
        synchronized(hosts) {
            List<Address> tmp=hosts.get(host);
            suspects=tmp != null? new ArrayList<Address>(tmp) : null;
        }
        if(suspects != null) {
            log.debug("%s: unsuspecting host %s; unsuspected members: %s", local_addr, host, Util.printListWithDelimiter(suspects, ","));
            for(Address unsuspect: suspects)
                unsuspect(unsuspect);
        }
    }*/

    protected boolean unsuspect(Address mbr) {
        if(mbr == null) return false;
        boolean do_unsuspect;
        synchronized(this) {
            do_unsuspect=!suspected_mbrs.isEmpty() && suspected_mbrs.remove(mbr);
            if(do_unsuspect)
                has_suspected_mbrs=!suspected_mbrs.isEmpty();
        }
        if(do_unsuspect) {
            up_prot.up(new Event(Event.UNSUSPECT, mbr));
            down_prot.down(new Event(Event.UNSUSPECT, mbr));
        }
        return do_unsuspect;
    }


    protected String _printTimestamps() {
        StringBuilder sb=new StringBuilder();
        long current_time=getTimestamp();
        for(Map.Entry<InetAddress,Long> entry: timestamps.entrySet()) {
            sb.append(entry.getKey()).append(": ");
            sb.append(TimeUnit.SECONDS.convert(current_time - entry.getValue(), TimeUnit.NANOSECONDS)).append(" secs old\n");
        }
        return sb.toString();
    }

    protected void updateTimestampFor(InetAddress host) {
        timestamps.put(host, getTimestamp());
    }

    /** Returns the age (in secs) of the given host */
    protected long getAgeOf(InetAddress host) {
        Long ts=timestamps.get(host);
        return ts != null? TimeUnit.SECONDS.convert(getTimestamp() - ts, TimeUnit.NANOSECONDS) : -1;
    }

    protected long getTimestamp() {
        return use_time_service && time_service != null? time_service.timestamp() : System.nanoTime();
    }






    /** Selected members run this task periodically. The task pings all hosts except self using ping_command.
     * When a host is not seen as alive, all members associated with that host are suspected */
    protected class PingTask implements Runnable {

        public void run() {
            List<InetAddress> targets;
            synchronized(hosts) {
                targets=new ArrayList<>(hosts.keySet());
            }
            targets.remove(local_host);

            for(InetAddress target: targets) {
                try {
                    // Ping each host
                    boolean is_alive=ping_command.isAlive(target, check_timeout);
                    num_liveness_checks++;
                    if(is_alive)
                        updateTimestampFor(target); // skip the timestamp check, as this host is alive
                    else {
                        log.trace("%s: %s is not alive (age=%d secs)",local_addr,target,getAgeOf(target));

                        // Check timestamp - we didn't get a response to the liveness check
                        long current_time=getTimestamp();
                        long timestamp=timestamps.get(target);
                        long diff=TimeUnit.MILLISECONDS.convert(current_time - timestamp,TimeUnit.NANOSECONDS);
                        if(diff >= timeout)
                            suspect(target);
                    }
                }
                catch(Exception e) {
                    log.error(local_addr + ": ping command failed", e);
                }
            }
        }
    }


    /** Command used to check whether a given host is alive, periodically called */
    public interface PingCommand {
        /**
         * Checks whether a given host is alive
         * @param host The host to be checked for liveness
         * @param timeout Number of milliseconds to wait for the check to complete
         * @return true if the host is alive, else false
         */
        boolean isAlive(InetAddress host, long timeout) throws Exception;
    }


    public static class IsReachablePingCommand implements PingCommand {
        public boolean isAlive(InetAddress host, long timeout) throws Exception {
            return host.isReachable((int)timeout);
        }
    }

    protected static class ExternalPingCommand implements PingCommand {
        protected final String cmd;

        public ExternalPingCommand(String cmd) {
            this.cmd=cmd;
        }

        public boolean isAlive(InetAddress host, long timeout) throws Exception {
            return CommandExecutor2.execute(cmd + " " + host.getHostAddress()) == 0;
        }
    }

    public static class CommandExecutor {

        public static int execute(String command) throws Exception {
            Process p=Runtime.getRuntime().exec(command);
            InputStream in=p.getInputStream(), err=p.getErrorStream();
            try {
                Reader in_reader, err_reader;
                in_reader=new Reader(in);
                err_reader=new Reader(err);
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

            Reader(InputStream in) {
                this.in=new InputStreamReader(in);
            }

            public void run() {
                int c;
                while(true) {
                    try {
                        c=in.read();
                        if(c == -1)
                            break;
                        // System.out.print((char)c);
                    }
                    catch(IOException e) {
                        break;
                    }
                }
            }
        }
    }

    public static class CommandExecutor2 {
        public static int execute(String command) throws Exception {
            Process p=Runtime.getRuntime().exec(command);
            return p.waitFor();
        }
    }
}

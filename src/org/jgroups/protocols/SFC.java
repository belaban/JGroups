package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.jgroups.util.Streamable;
import org.jgroups.util.BoundedList;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;
import EDU.oswego.cs.dl.util.concurrent.CondVar;
import EDU.oswego.cs.dl.util.concurrent.ReentrantLock;
import EDU.oswego.cs.dl.util.concurrent.Sync;

import java.util.*;
import java.io.*;

/**
 * Simple flow control protocol. After max_credits bytes sent to the group (or an individual member), the sender blocks
 * until it receives an ack from all members that they indeed received max_credits bytes.
 * Design in doc/design/SimpleFlowControl.txt
 * @author Bela Ban
 * @version $Id: SFC.java,v 1.10.2.6 2007/04/18 05:08:31 bstansberry Exp $
 */
public class SFC extends Protocol {
   
    static final String name="SFC";
    
    private static final Object NULL = new Object();
    
    /** Max number of bytes to send per *multicast* receiver until an ack must 
     * be received before continuing sending. */
    private long multicast_max_credits=2000000;

    /** Factor to multiply <tt>multicast_max_credits</tt> by to derive
     * <tt>multicast_min_credits</tt>. Ignored if <tt>multicast_min_credits</tt>
     * is explicitly set. */
    private double multicast_min_threshold=0.20;

    /** If multicast bytes received rise past this limit, we send more credits 
     * to the sender. Computed as <tt>multicast_max_credits</tt> times 
     * <tt>min_theshold</tt>. If explicitly set, this will override the above 
     * computation */
    private long multicast_min_credits=0;
    
    /** Whether to apply flow control to unicast messages.  Set this to false
     * in a TCP-based channel  */
    private boolean control_unicast = true;

    /** Max number of bytes per unicast receiver. Defaults to the
     * value of multicast_max_credits if not specifically set. */
    private long unicast_max_credits = 0;

    /** Factor to multiply <tt>unicast_max_credits</tt> by to derive
     * <tt>unicast_min_credits</tt> . Defaults to the value of 
     * <tt>multicast_min_threshold</tt> if not explicitly set.  Ignored if 
     * <tt>unicast_min_credits</tt> is explicitly set.*/
    private double unicast_min_threshold=0.20;

    /** If unicast bytes received rise past this limit, we send more credits to  
     * the sender. Computed as <tt>unicast_max_credits</tt> times 
     * <tt>unicast_min_theshold</tt>. If explicitly set, this will override the 
     * above computation */
    private long unicast_min_credits=0;

    /** Number of milliseconds after which we send a new credit request 
     * if we are waiting for credit responses */
    private long max_block_time=5000;
    
    /** Minimum interval between credit requests sent by threads that have
     * just woken up after waiting max_block_time. Used to prevent spamming 
     * the group if numerous threads block for max_block_time and then one 
     * after another in a short interval awaken to request credit. */
    private long min_credit_request_interval = 250L;

    /** Whether an up thread that comes back down should be allowed to
     * bypass blocking if all credits are exhausted. Avoids JGRP-465. */
    private boolean ignore_synchronous_response = true;
    
    /** Thread that carries *messages* through up() and shouldn't be 
     * blocked in down() if ignore_synchronous_response==true. JGRP-465. */
    private Thread ignore_thread;
    
    /** Coordinates credit availability for multicast messages */
    private FlowCoordinator multicast_coordinator;
    
    /** Map<Address, FlowCoordinator> of the coordinators for each peer */
    private Map unicast_coordinators = new ConcurrentReaderHashMap();

    private final List members=new LinkedList();

    private boolean running=true;

    // ---------------------- Management information -----------------------
    
    /** Last 50 blockings. Shared between FlowCoordinators since it's
     * thread safe. */
    final BoundedList blockings=new BoundedList(50);

    public void resetStats() {
        super.resetStats();
        
        Set coords = getFlowCoordinators();
        for (Iterator it = coords.iterator(); it.hasNext();)
           ((FlowCoordinator) it.next()).resetStats();
        blockings.removeAll();
    }

    public long getMulticastMaxCredits() {return multicast_max_credits;}
    public long getUnicastMaxCredits() {return unicast_max_credits; }
    
    public long getMulticastCredits() {
       if (multicast_coordinator != null)
          return multicast_coordinator.curr_credits_available;
       else
          return multicast_max_credits;
    }
    
    public long getBytesSent() {
       long num_bytes_sent = 0;
       Set coords = getFlowCoordinators();
       for (Iterator it = coords.iterator(); it.hasNext(); )
          num_bytes_sent += ((FlowCoordinator) it.next()).num_bytes_sent;
       return num_bytes_sent;
    }    
    public long getBlockings() {
       long num_blockings = 0;
       Set coords = getFlowCoordinators();
       for (Iterator it = coords.iterator(); it.hasNext(); )
          num_blockings += ((FlowCoordinator) it.next()).num_blockings;
       return num_blockings;
    }
    public long getCreditRequestsSent() {
       long num_credit_requests_sent = 0;
       Set coords = getFlowCoordinators();
       for (Iterator it = coords.iterator(); it.hasNext(); )
          num_credit_requests_sent += ((FlowCoordinator) it.next()).num_credit_requests_sent;
       return num_credit_requests_sent;
    }
    public long getCreditRequestsReceived() {
       long num_credit_requests_received = 0;
       Set coords = getFlowCoordinators();
       for (Iterator it = coords.iterator(); it.hasNext(); )
          num_credit_requests_received += ((FlowCoordinator) it.next()).num_credit_requests_received;
       return num_credit_requests_received;
    }
    public long getReplenishmentsReceived() {
       long num_replenishments_received = 0;
       Set coords = getFlowCoordinators();
       for (Iterator it = coords.iterator(); it.hasNext(); )
          num_replenishments_received += ((FlowCoordinator) it.next()).num_replenishments_received;
       return num_replenishments_received;
    }
    public long getReplenishmentsSent() {
       long num_replenishments_sent = 0;
       Set coords = getFlowCoordinators();
       for (Iterator it = coords.iterator(); it.hasNext(); )
          num_replenishments_sent += ((FlowCoordinator) it.next()).num_replenishments_sent;
       return num_replenishments_sent;
    }
    public long getTotalBlockingTime() {
       long total_block_time = 0;
       Set coords = getFlowCoordinators();
       for (Iterator it = coords.iterator(); it.hasNext(); )
          total_block_time += ((FlowCoordinator) it.next()).total_block_time;
       return total_block_time;
    }    
    public double getAverageBlockingTime() {
       long num_blockings = getBlockings();
       if (num_blockings == 0)
          return 0;
       else
          return getTotalBlockingTime() / num_blockings;
    }
    
    public long getCredits(Address peer) {
       FlowCoordinator coord = getFlowCoordinator(peer);
       return coord == null ? unicast_max_credits : coord.curr_credits_available;
    }
    public long getBytesSent(Address peer) {
       FlowCoordinator coord = getFlowCoordinator(peer);
       return coord == null ? 0 : coord.num_bytes_sent;
    }    
    public long getBlockings(Address peer) {
       FlowCoordinator coord = getFlowCoordinator(peer);
       return coord == null ? 0 : coord.num_blockings;
    }
    public long getCreditRequestsSent(Address peer) {
       FlowCoordinator coord = getFlowCoordinator(peer);
       return coord == null ? 0 : coord.num_credit_requests_sent;
    }
    public long getCreditRequestsReceived(Address peer) {
       FlowCoordinator coord = getFlowCoordinator(peer);
       return coord == null ? 0 : coord.num_credit_requests_received;
    }
    public long getReplenishmentsReceived(Address peer) {
       FlowCoordinator coord = getFlowCoordinator(peer);
       return coord == null ? 0 : coord.num_replenishments_received;
    }
    public long getReplenishmentsSent(Address peer) {
       FlowCoordinator coord = getFlowCoordinator(peer);
       return coord == null ? 0 : coord.num_replenishments_sent;
    }
    public long getTotalBlockingTime(Address peer) {
       FlowCoordinator coord = getFlowCoordinator(peer);
       return coord == null ? 0 : coord.total_block_time;
    }    
    public double getAverageBlockingTime(Address peer) {
       FlowCoordinator coord = getFlowCoordinator(peer);
       return coord == null ? 0 : coord.getAverageBlockingTime();
    }

    public Map dumpStats() {
        Map retval=super.dumpStats();
        if(retval == null)
            retval=new HashMap();
        return retval;
    }

    public String printBlockingTimes() {       
        return blockings.toString();
    }

    public String printReceived() {
      StringBuffer sb = new StringBuffer();
      Set coords = getFlowCoordinators();
      for (Iterator it = coords.iterator(); it.hasNext(); ) {
         sb.append(((FlowCoordinator) it.next()).printReceived());
         sb.append('\n');
      }      
      return sb.toString();
    }

    public String printPendingCreditors() {
       StringBuffer sb = new StringBuffer();
       Set coords = getFlowCoordinators();
       for (Iterator it = coords.iterator(); it.hasNext(); ) {
          sb.append(((FlowCoordinator) it.next()).printPendingCreditors());
          sb.append('\n');
       }      
       return sb.toString();
    }

    public String printPendingRequesters() {
       StringBuffer sb = new StringBuffer();
       Set coords = getFlowCoordinators();
       for (Iterator it = coords.iterator(); it.hasNext(); ) {
          sb.append(((FlowCoordinator) it.next()).printPendingRequesters());
          sb.append('\n');
       }      
       return sb.toString();
    }

    public void unblock() {
       Set coords = getFlowCoordinators();
       for (Iterator it = coords.iterator(); it.hasNext(); ) {
          ((FlowCoordinator) it.next()).unblock();
       }
    }

    // ------------------- End of management information ----------------------


    public final String getName() {
        return name;
    }

    public boolean setProperties(Properties props) {
        String  str;
        super.setProperties(props);
        
        boolean min_credits_set = false;

        str=props.getProperty("max_block_time");
        if(str != null) {
            max_block_time=Long.parseLong(str);
            props.remove("max_block_time");
        }
        
        str=props.getProperty("min_credit_request_interval");
        if(str != null) {
            min_credit_request_interval=Long.parseLong(str);
            props.remove("min_credit_request_interval");
        }

        str=props.getProperty("max_credits");
        if(str != null) {
            multicast_max_credits=Long.parseLong(str);
            props.remove("max_credits");
        }

        Util.checkBufferSize("SFC.max_credits", multicast_max_credits);

        str=props.getProperty("min_threshold");
        if(str != null) {
            multicast_min_threshold=Double.parseDouble(str);
            props.remove("min_threshold");
        }

        str=props.getProperty("min_credits");
        if(str != null) {
            multicast_min_credits=Long.parseLong(str);
            props.remove("min_credits");
            min_credits_set=true;
        }

        if(!min_credits_set)
            multicast_min_credits=(long)((double)multicast_max_credits * multicast_min_threshold);
        
        str=props.getProperty("control_unicast");
        if(str != null) {
           control_unicast=Boolean.valueOf(str).booleanValue();
           props.remove("control_unicast");
        }

        str=props.getProperty("unicast_max_credits");
        if(str != null) {
            unicast_max_credits=Long.parseLong(str);
            props.remove("unicast_max_credits");
        }

        if (unicast_max_credits == 0)
           unicast_max_credits = multicast_max_credits;
        
        Util.checkBufferSize("SFC.unicast_max_credits", multicast_max_credits);

        str=props.getProperty("unicast_min_threshold");
        if(str != null) {
           unicast_min_threshold=Double.parseDouble(str);
           props.remove("unicast_min_threshold");
        }
        else {
           unicast_min_threshold = multicast_min_threshold;
        }

        min_credits_set = false;
        
        str=props.getProperty("unicast_min_credits");
        if(str != null) {
            unicast_min_credits=Long.parseLong(str);
            props.remove("unicast_min_credits");
            min_credits_set=true;
        }

        if(!min_credits_set)
           unicast_min_credits=(long)((double)unicast_max_credits * unicast_min_threshold);
        
        str=props.getProperty("ignore_synchronous_response");
        if(str != null) {
           ignore_synchronous_response=Boolean.valueOf(str).booleanValue();
           props.remove("ignore_synchronous_response");
        }
        
        if(!props.isEmpty()) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }



    public void down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Address dest=msg.getDest();
                
                FlowCoordinator coord = getFlowCoordinator(dest);
                if (coord != null) {
                   if (coord.handleDownMessage(evt, msg)) {
                      // Message already passed down as part of credit
                      // request process, so just return
                      return;
                   }
                   // else pass down normally
                }
                break;

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            case Event.SUSPECT:
                handleSuspect((Address)evt.getArg());
                break;
        }

        passDown(evt);
    }



    public void up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
               
               // JGRP-465. We only deal with msgs to avoid having to use
               // a concurrent collection; ignore views, suspicions, etc 
               // which can come up on unusual threads.
               if (ignore_thread == null && ignore_synchronous_response)
                  ignore_thread = Thread.currentThread();
                
                Message msg=(Message)evt.getArg();
                Address sender=msg.getSrc();                
                
                Header hdr=(Header)msg.getHeader(name);
                if(hdr != null) {
                    FlowCoordinator coord;
                    switch(hdr.type) {
                        case Header.MULTICAST_CREDIT_REQUEST:
                            multicast_coordinator.handleCreditRequest(sender);
                            break;
                        case Header.MULTICAST_REPLENISH:
                            multicast_coordinator.handleCreditResponse(sender, (Number) msg.getObject());
                            break;
                        case Header.UNICAST_CREDIT_REQUEST:
                           coord = getFlowCoordinator(sender);
                           if (coord != null)
                              coord.handleCreditRequest(sender);
                           break;
                        case Header.UNICAST_REPLENISH:
                           coord = getFlowCoordinator(sender);
                           if (coord != null)
                              coord.handleCreditResponse(sender, (Number) msg.getObject());
                           break;
                        default:
                            if(log.isErrorEnabled())
                                log.error("unknown header type " + hdr.type);
                            break;
                    }
                    return; // we don't pass the request further up
                }
                
                Address dest = msg.getDest();
                if (dest == null || dest.isMulticastAddress()) {
                   multicast_coordinator.handleUpMessage(msg, sender);
                }
                else {
                   FlowCoordinator coord = getFlowCoordinator(sender);
                   if (coord != null)
                      coord.handleUpMessage(msg, sender);
                }
                break;

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            case Event.SUSPECT:
                handleSuspect((Address)evt.getArg());
                break;
        }
        
        passUp(evt);
    }

    
    public void start() throws Exception {
        super.start();
        if (multicast_coordinator == null)
           multicast_coordinator = new FlowCoordinator(null, multicast_max_credits, multicast_min_credits);
        
        if (control_unicast && unicast_coordinators == null)
           unicast_coordinators = new ConcurrentReaderHashMap();
        
        ignore_thread = null;
        
        running=true;
    }


    public void stop() {
        super.stop();
        running=false;
        
        for (Iterator it = getFlowCoordinators().iterator(); it.hasNext();) {
            ((FlowCoordinator) it.next()).stop();
        }
    }


    private void handleViewChange(View view) {
        List mbrs=view != null? view.getMembers() : null;
        if(mbrs != null) {
            synchronized(members) {
                members.clear();
                members.addAll(mbrs);
            }
        }
        
        multicast_coordinator.handleViewChange(view);
        
        if (control_unicast) {
           
           for (Iterator it = unicast_coordinators.values().iterator(); it.hasNext();)
           {
              ((FlowCoordinator) it.next()).handleViewChange(view);
           }
           
           unicast_coordinators.keySet().retainAll(members);
           
           for(Iterator it = members.iterator(); it.hasNext(); ) {
              Address mbr = (Address) it.next();
               if(!unicast_coordinators.containsKey(mbr))
               {
                  FlowCoordinator coord = new FlowCoordinator(mbr, unicast_max_credits, unicast_min_credits);
                  unicast_coordinators.put(mbr, coord);
                  coord.handleViewChange(view);
               }
           }
        }
    }


    private void handleSuspect(Address suspected_mbr) {
        // this is the same as a credit response - we cannot block forever for a crashed member
        Long credits = new Long(multicast_max_credits);
        multicast_coordinator.handleCreditResponse(suspected_mbr, credits);
        if (control_unicast) {
           FlowCoordinator coord = (FlowCoordinator) unicast_coordinators.get(suspected_mbr);
           credits = new Long(unicast_max_credits);
           if (coord != null)
              coord.handleCreditResponse(suspected_mbr, credits);
        }
    }
    
    private FlowCoordinator getFlowCoordinator(Address mbr)
    {
       FlowCoordinator coord = null;
       if (mbr == null || mbr.isMulticastAddress())
       {
          coord = multicast_coordinator;          
       }
       else if (control_unicast)
       {
         coord = (FlowCoordinator) unicast_coordinators.get(mbr); 
       }
       return coord;
    }
    
    private Set getFlowCoordinators()
    {
       Set coords = new HashSet();
       if (multicast_coordinator != null)
          coords.add(multicast_coordinator);
       if (control_unicast) {
          coords.addAll(unicast_coordinators.values());
       }
       
       return coords;
    }

    class FlowCoordinator
    {       
       final Address target;
       final boolean multicast;
       
       /** Max number of bytes to send per receiver until an ack must be 
        * received before continuing sending */
       private final long max_credits;

       private final long min_credits;
       
       /** Current number of credits available to send */
       private long curr_credits_available;
       
       /** Map<Address, Credit> which keeps track of credits available 
        * (only used if we are multicast) */
       private Map creditors;
       
       /** Array of the values in the creditors map (only used if we are multicast) */
       private Credit[] credits;
       
       /** Map which keeps track of bytes received from senders (only used 
        *  if we are multicast) */
       private Map received;
       
       /** Bytes received from our unicast target (only used if we are unicast) */
       private Credit unicast_received_count = null;
       
       /** Set of members which have requested credits but from whom we have
        * not yet received max_credits bytes (only used if we are unicast)*/
       private Set pending_requesters;

       /** Whether there is a credit request pending from our unicast target
        *  (only used if we are unicast) */
       private boolean request_pending;
       
       /** Set of members from whom we haven't yet received credits 
        * (only relevant if we are multicast) */
       private Set pending_creditors;
       
       private final Sync lock=new ReentrantLock();
       /** Lock protecting access to received and pending_requesters */
       private final Sync received_lock=new ReentrantLock();

       /** Used to wait for and signal when credits become available again */
       private final CondVar credits_available= new CondVar(lock);
       
       /** Last time a thread woke up from blocking and had to request credit */
       private long last_blocked_request = 0L;
       
       private boolean blocking;
       
       long start, stop;
       
       /** Just used in log messages */
       final String targetLabel;
       
       // Mgmt info
       private long num_blockings=0;
       private long num_bytes_sent=0;
       private long num_credit_requests_sent=0;
       private long num_credit_requests_received=0;
       private long num_replenishments_received=0;
       private long num_replenishments_sent=0;
       private long total_block_time=0;
       
       FlowCoordinator(Address target, long max_credits, long min_credits)
       {
          this.target = target;
          multicast = (target == null);
          this.max_credits = max_credits;
          this.min_credits = min_credits;
          curr_credits_available = max_credits;
          
          if (multicast) {
             pending_creditors=new HashSet();
             pending_requesters = new HashSet();
             received = new HashMap(12);
             creditors = new HashMap(12);
             credits = new Credit[0];
          }
          
          targetLabel = multicast ? "MULTICAST: " : target + ": ";
       }
       
       /**
        * 
        * @param evt Event that wraps msg
        * @param msg the msg
        * @return <code>true</code> if this method passed the message down
        *         the stack; <code>false</code> if not (normally false)
        */
       boolean handleDownMessage(Event evt, Message msg)
       {
          boolean send_credit_request=false;
          Util.lock(lock);
          try {             
              boolean bypass = false;
              while(curr_credits_available <=0 && running) {
                 
                  // JGRP-465. Don't block the single up_thread.
                  if (ignore_synchronous_response 
                        && ignore_thread == Thread.currentThread()) {
                  
                     if (trace) {
                        log.trace(targetLabel + "Bypassing blocking to avoid " +
                                "deadlocking " + Thread.currentThread());
                     }
                     
                     // Skip blocking, but still decrement credit below. This
                     // privileges incoming RPC calls at the expense of outgoing,
                     // but not decrementing increases OOM risk
                     bypass = true;
                     break;
                  }
                  
                  if(trace)
                      log.trace(targetLabel + "blocking (current credits=" + 
                                curr_credits_available + ")");                  
                  try {
                      startBlocking(); // ignored if already blocking
                      num_blockings++;
                      // will be signalled when we have credit responses from all members
                      credits_available.timedwait(max_block_time);
                      if(curr_credits_available <=0 && running)
                      {
                          if (trace) {
                             log.trace(targetLabel + "Returned from timedwait " +
                                    "but curr_credits_available=" +
                                    curr_credits_available);
                          }
                          
                          boolean urgentRequest = false;
                          if (min_credit_request_interval > 0) {
                             long now = System.currentTimeMillis();
                             if (now - last_blocked_request > min_credit_request_interval) {
                                last_blocked_request = now;
                                urgentRequest = true;
                             }
                          }
                          else {
                             urgentRequest = true;
                          }
                          
                          if (urgentRequest) {
                             // Release the lock to avoid the JGRP-292 problem
                             // Doing this with the lock released should be safe
                             // as all it does is create a stateless message
                             // and push it down the stack
                             Util.unlock(lock);
                             try {
                                sendCreditRequest(target, multicast);
                             }
                             finally {
                                 Util.lock(lock);
                             }
                          }
                      }
                  }
                  catch(InterruptedException e) {
                      if(warn)
                          log.warn("thread was interrupted", e);
                      Thread.currentThread().interrupt(); // pass the exception on to the  caller
                      return false;
                  }
              }
              
              // when we get here, curr_credits_available is guaranteed to be > 0
              int len=msg.getLength();
              num_bytes_sent+=len;
              decrementCredits(len); // if this puts us below 0, we'll block on 
                                     // insufficient credits on the next down() call
   
              if (!bypass && curr_credits_available <=0) {
                 send_credit_request=true;
              }
          }
          finally {
              Util.unlock(lock);
          }
   
          // we don't need to protect send_credit_request because a thread above either (a) decrements the credits
          // by the msg length and sets send_credit_request to true or (b) blocks because there are no credits
          // available or (c) sets the bypass flag and thus doesn't set send_credit_request=true. 
          // So only 1 thread can ever set send_credit_request=true at any given time
          if(send_credit_request) {
              passDown(evt);       // send the message before the credit request
              sendCreditRequest(target, multicast); // do this outside of the lock
              return true;
          }
          
          return false;
       }
       
       private void startBlocking() {
          if (!blocking) {
             start=System.nanoTime();
             blocking = true;
          }
       }
       
       private void stopBlocking() {
          if (blocking) {
             stop=System.nanoTime();
             long diff=(stop-start)/1000000L;
             if(trace) {
                 log.trace(targetLabel + "stopped blocking (total " +
                           "blocking time=" + diff + " ms)");
             }
             blockings.add(new Long(diff));
             total_block_time+=diff;
             blocking = false;
          }
       }
       

       private void decrementCredits(long msgLength) {
          if (multicast) {
             long lowest = Long.MAX_VALUE;
             for (int i = 0; i < credits.length; i++) {
                credits[i].amount -= msgLength;
                if (credits[i].amount < lowest)
                   lowest = credits[i].amount;
             }
             curr_credits_available = lowest;
          }
          else {
             curr_credits_available -= msgLength;
          }
       }
       
       
       void handleUpMessage(Message msg, Address sender) {
           int len=msg.getLength(); // we don't care about headers, this is faster than size()

           long credit_response = 0;
           boolean urgent = false;

           Util.lock(received_lock);
           try {
               Credit credit=getReceivedCount(sender);
               if(credit == null) {
                   credit = new Credit(0);
                   setReceivedCount(sender, credit);
               }
               
               credit.amount += len;
               
               // If we've received min_credits, send some back
               if(credit.amount >= min_credits) {
                   if (trace) {
                      log.trace(targetLabel + "credits below minimum; sending " + 
                                credit.amount +  " credits to " + sender);
                   }
                   credit_response = credit.amount;
                   credit.amount = 0;
                   
                   if (isRequestPending(sender)) {
                      setRequestPending(sender ,false);
                      urgent = true;
                   }
               }
               // see whether we have any pending credit requests
               else if (credit.amount > 0 && isRequestPending(sender)) {
                  credit_response = credit.amount;
                  credit.amount = 0;
                  setRequestPending(sender, false);
                  urgent = true;
                  if (trace) {
                     log.trace(targetLabel + "sender had a credit request " +
                            "pending; sending " + credit_response +  
                            " credits to " + sender);
                  }
               }
           }
           finally {
               Util.unlock(received_lock);
           }

           // Send outside of the monitor
           if(credit_response > 0) 
               sendCreditResponse(sender, credit_response, urgent, multicast);
       }
       
       private Credit getReceivedCount(Address sender) {
          if (multicast)
             return (Credit) received.get(sender);
          else
             return unicast_received_count;          
       }
       
       private void setReceivedCount(Address sender, Credit credit) {
          if (multicast)
             received.put(sender, credit);
          else
             unicast_received_count = credit;
       }
       
       private boolean isRequestPending(Address sender) {
          if (multicast)
             return !pending_requesters.isEmpty() && pending_requesters.contains(sender);
          else
             return request_pending;
       }
       
       private void setRequestPending(Address sender, boolean pending) {
          if (multicast) {
             if (pending)
                pending_requesters.add(sender);
             else
                pending_requesters.remove(sender);
          }
          else {
             request_pending = pending;
          }
       }

       void handleCreditRequest(Address sender) {
           
           long credit_response = 0;
           Credit credit = null;
           Util.lock(received_lock);
           try {
               num_credit_requests_received++;
               credit=getReceivedCount(sender);
               if(trace)
                   log.trace(targetLabel + "received credit request from " + sender + 
                            " (total received: " + credit.amount + " bytes)");

               if(credit == null) {                   
                   credit = new Credit(0);
                   setReceivedCount(sender, credit);
               }
               
               if (credit.amount > 0) {
                  credit_response = credit.amount;
                  credit.amount = 0;
               }
               else {
                  if(trace) {
                     log.trace(targetLabel + "no credits available to send to " + 
                           sender + " -- marking credit request as pending");
                  }
                  setRequestPending(sender, true);
               }
           }
           finally{
               Util.unlock(received_lock);
           }
           
           if(credit_response > 0) {
               if(trace) {
                  log.trace(targetLabel + "sending " + credit_response + 
                        " credits to " + sender);
               }
               sendCreditResponse(sender, credit_response, true, multicast);
           }
       }

       void handleCreditResponse(Address sender, Number credits) {           
           Util.lock(lock);
           try {              
               num_replenishments_received++;
               if (multicast) {
                  Credit current = (Credit) creditors.get(sender);
                  
                  if (current == null) {
                     // sender was removed from the view but we just now
                     // got a suspect message; ignore it
                     return;
                  }
                     
                  // This credit response will cause curr_credits_available to
                  // increase only if credit with this sender equals 
                  // curr_credits_available, so only bother recalculating
                  // curr_credits_available in that case
                  boolean updateMin = (current.amount <= curr_credits_available);
                  
                  current.amount += credits.longValue();
                  if (updateMin)
                     determineCreditsAvailable();
                  
                  if (trace) {
                     log.trace(targetLabel + "received " + credits + 
                           " from " + sender + "; sender balance=" +
                           current.amount + "; curr_credits_available=" +
                           curr_credits_available);
                  }
               }
               else {
                  curr_credits_available += credits.longValue();
                  
                  if (trace) {
                     log.trace(targetLabel + "received " + credits + 
                           " from " + sender + "; curr_credits_available=" +
                           curr_credits_available);
                  }
               }
               
               if(blocking && curr_credits_available > 0) {
                   // Do bookeeping of block time
                   stopBlocking();
                   credits_available.broadcast();
               }
           }
           finally{
               Util.unlock(lock);
           }
       }
       
       private void determineCreditsAvailable() {
          long lowest =Long.MAX_VALUE;
          for (int i = 0; i < credits.length; i++) {
             if (credits[i].amount < lowest)
                lowest = credits[i].amount;
          }
          curr_credits_available = lowest;
       }

       void handleViewChange(View view) {
          
           Util.lock(lock);
           try {
               if (multicast) {
                  
                  // remove all members which left from creditors
                  creditors.keySet().retainAll(members);
                  
                  // Add new members
                  for (Iterator it = members.iterator(); it.hasNext();) {
                     Address mbr = (Address) it.next();
                     if (!creditors.containsKey(mbr)) {
                        creditors.put(mbr, new Credit(max_credits));
                     }
                  }
                  
                  // Reset the credits array
                  credits = new Credit[creditors.size()];
                  credits = (Credit[]) creditors.values().toArray(credits);
                  
                  // See if the view change removed the peer(s) who were
                  // preventing sending messages
                  determineCreditsAvailable();                  
               }
               else if (!members.contains(target)) {
                  // I'm about to be disposed of. Make sure I don't block any
                  // threads.
                  curr_credits_available = Long.MAX_VALUE;
                  if(trace)
                     log.trace(targetLabel + "target removed from view; releasing all threads");
               }
               
               if (blocking && curr_credits_available > 0) {
                   if(trace)
                       log.trace(targetLabel + "as a result of view change, " +
                             "replenished credits to " + curr_credits_available);
                   stopBlocking();
                   credits_available.broadcast();
               }
           }
           finally {
               Util.unlock(lock);
           }

           if (multicast) {
              Util.lock(received_lock);
              try {
                  // remove left members
                  received.keySet().retainAll(members);
                  pending_requesters.retainAll(members);
              }
              finally{
                  Util.unlock(received_lock);
              }
           }
       }
       
       void stop() {
          Util.lock(lock);
          try {
              credits_available.broadcast();
          }
          finally {
              Util.unlock(lock);
          }
       }

       // ---------------------- Management information -----------------------
       
       long getBytesSent() {return num_bytes_sent;}
       long getBlockings() {return num_blockings;}
       long getCreditRequestsSent() {return num_credit_requests_sent;}
       long getCreditRequestsReceived() {return num_credit_requests_received;}
       long getReplenishmentsReceived() {return num_replenishments_received;}
       long getReplenishmentsSent() {return num_replenishments_sent;}
       long getTotalBlockingTime() {return total_block_time;}
       double getAverageBlockingTime() {
          return num_blockings == 0? 0 : total_block_time / num_blockings;
       }

       void resetStats() {
           num_blockings=total_block_time=num_replenishments_received=num_credit_requests_sent=num_bytes_sent=0;
           num_replenishments_sent=num_credit_requests_received=0;
       }

       String printReceived() {        
         try
         {
           received_lock.acquire();
           try {
               return targetLabel + received.toString();
           }
           finally {
              Util.release(received_lock);
           }
         }
         catch (InterruptedException e)
         {
            String msg = "Interrupted while acquiring received_lock";
            if (warn)
               log.warn(targetLabel + "printReceived(): " + msg);
            return targetLabel + msg;
         }
       }

       String printPendingCreditors() {
          try
          {
             lock.acquire();
              try {
                  return targetLabel + pending_creditors.toString();
              }
              finally {
                 Util.release(lock);
              }
          }
          catch (InterruptedException e)
          {
             String msg = "Interrupted while acquiring lock";
             if (warn)
                log.warn(targetLabel + "printPendingCreditors(): " + msg);
             return targetLabel + msg;
          }
       }

       String printPendingRequesters() {
          try
          {
             received_lock.acquire();
              try {
                  return targetLabel + pending_requesters.toString();
              }
              finally {
                 Util.release(received_lock);
              }
          }
          catch (InterruptedException e)
          {
             String msg = "Interrupted while acquiring received_lock";
             if (warn)
                log.warn(targetLabel + "printPendingRequesters(): " + msg);
             return targetLabel + msg;
          }
       }

       void unblock() {
          Util.lock(lock);
          try {
               curr_credits_available=max_credits;
               credits_available.broadcast();
           }
           finally {
              Util.unlock(lock);
           }
       }

       private void sendCreditRequest(Address dest, boolean multicast) {
           Message credit_req=new Message(dest);
           // OOB not available in 2.4. In 2.5, sending OOB *is* OK though,
           // as JGRP-473 fix means the response will not leak memory
           // credit_req.setFlag(Message.OOB);
           byte type = (dest == null) ? Header.MULTICAST_CREDIT_REQUEST 
                                      : Header.UNICAST_CREDIT_REQUEST;
           credit_req.putHeader(name, new Header(type));
           if(trace)
              log.trace(targetLabel + "sending credit request to " + 
                       (multicast ? "group" : dest.toString()));
           num_credit_requests_sent++;
           passDown(new Event(Event.MSG, credit_req));
       }

       private void sendCreditResponse(Address dest, long credit, boolean urgent, boolean multicast) {
          
          Number obj;
          // For some reason 1.4 compiler complains if I use a ternary op here
          if (credit < Integer.MAX_VALUE)
             obj = new Integer((int) credit);
          else
             obj = new Long(credit);
          Message credit_rsp=new Message(dest, null, obj);
          // OOB not available in 2.4; uncomment in 2.5
//          if (urgent)
//             credit_rsp.setFlag(Message.OOB);
           Header hdr=new Header(multicast ? Header.MULTICAST_REPLENISH : Header.UNICAST_REPLENISH);
           credit_rsp.putHeader(name, hdr);           
           num_replenishments_sent++;
           passDown(new Event(Event.MSG, credit_rsp));
       }
    }
    
    /** Simple data class that wraps a long */
    private static class Credit {       
       long amount;
       
       Credit(long credit) {
          this.amount = credit;
       }
    }

    public static class Header extends org.jgroups.Header implements Streamable {
        public static final byte MULTICAST_CREDIT_REQUEST = 1; // the sender of the message is the requester
        public static final byte MULTICAST_REPLENISH      = 2; // the sender of the message is the creditor
        public static final byte UNICAST_CREDIT_REQUEST = 3;
        public static final byte UNICAST_REPLENISH      = 4;

        byte  type=MULTICAST_CREDIT_REQUEST;

        public Header() {

        }

        public Header(byte type) {
            this.type=type;
        }

        public long size() {
            return Global.BYTE_SIZE;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(type);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readByte();
        }

        public String toString() {
            switch(type) {
                case MULTICAST_REPLENISH: return "MULTICAST_REPLENISH";
                case MULTICAST_CREDIT_REQUEST: return "MULTICAST_CREDIT_REQUEST";
                case UNICAST_REPLENISH: return "UNICAST_REPLENISH";
                case UNICAST_CREDIT_REQUEST: return "UNICAST_CREDIT_REQUEST";
                default: return "<invalid type>";
            }
        }
    }

}

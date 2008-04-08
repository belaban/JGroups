
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Streamable;

import java.io.*;
import java.util.*;

/** <p>
 * Implements casual ordering layer using vector clocks.
 * </p>
 * <p>
 * Causal protocol layer guarantees that if message m0 multicasted
 * by a process group member p0 causes process group member
 * p1 to multicast message m1 then all other remaining process group
 * members in a current view will receive messages in order m0
 * followed by m1.
 * </p>
 * <p>
 * First time encountered, causal order seems very similar to FIFO order but
 * there is an important distinction.  While FIFO order gurantees that
 * if process group member p0 multicasts m0 followed by m1 the messages
 * will be delivered in order m0,m1 to all other group members, causal
 * order expands this notion of an order from a single group member "space"
 * to a whole group space i.e if p0 sends message m0 which causes member
 * p1 to send message m1 then all other group members are guaranteed to
 * receive m0 followed by m1.
 * </p>
 * <p>
 * Causal protocol layer achieves this ordering type by introducing sense of
 * a time in a group using vector clocks.  The idea is very simple. Each message
 * is labeled by a vector, contained in a causal header, representing the number of
 * prior causal messages received by the sending group member. Vector time of [3,5,2,4] in
 * a group of four members [p0,p1,p2,p3] means that process p0 has sent 3 messages
 * and has received 5,2 and 4 messages from a member p1,p2 and p3 respectively.
 * </p>
 * <p>
 * Each member increases its counter by 1 when it sends a message. When receiving
 * message mi from a member pi , (where pi != pj) containing vector time VT(mi),
 * process pj delays delivery of a message mi until:
 * </p>
 * <p>
 * for every k:1..n
 *
 *                VT(mi)[k] == VT(pj)[k] + 1    if k=i,
 *                VT(mi)[k] <= VT(pj)[k]        otherwise
 * </p>
 * <p>
 * After the next causal message is delivered at process group pj, VT(pj) is
 * updated as follows:
 *</p>
 *<p>
 *    for every k:1...n VT(pj)[k] == max(VT(mi)[k],VT(pj)[k])
 *</p>
 * <em>Note that this protocol is experimental and has never been tested extensively !</em>
 *  @author Vladimir Blagojevic vladimir@cs.yorku.ca
 *  @version $Id: CAUSAL.java,v 1.20 2008/04/08 14:51:21 belaban Exp $
 *
 **/

@Experimental
public class CAUSAL extends Protocol
{

    public static final class CausalHeader extends Header implements Streamable {
        
        /**
         * Comment for <code>serialVersionUID</code>
         */
        private static final long serialVersionUID = 3760846744526927667L;
        
        /**
         * vector timestamp of this header/message
         */
        private TransportedVectorTime t = null;
        
        /**
         *used for externalization
         */
        public CausalHeader() {
        }
        
        public CausalHeader(TransportedVectorTime timeVector) {
            t = timeVector;
        }
        
        /**
         *Returns a vector timestamp carreid by this header
         *@return Vector timestamp contained in this header
         */
        public TransportedVectorTime getVectorTime() {
            return t;
        }
        
        /**
         * Size of this vector timestamp estimation, used in fragmetation
         * @return headersize in bytes
         */
        public int size()
        {
            int retval=Global.BYTE_SIZE;
            if(t == null)
                return retval;
            retval+=t.senderPosition;
            if(t.values != null) {
                retval+=t.values.length * Global.INT_SIZE;
            }
            return retval;
        }
        
        /**
         * Manual serialization
         */
        public void writeExternal(ObjectOutput out) throws IOException
        {
            out.writeObject(t);
        }
        
        /**
         * Manual deserialization
         */
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
        {
            t = (TransportedVectorTime) in.readObject();
        }
        
        public void writeTo(DataOutputStream out) throws IOException {
            if(t == null)
            {
                out.writeBoolean(false);
                return;
            }
            out.writeBoolean(true);

            out.writeInt(t.senderPosition);
            
            int values[]=t.values;
            
            int len=values.length;
            out.writeInt(len);
            for(int i=0;i<len;i++) out.writeInt(values[i]);
        }
        
        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            if(in.readBoolean() == false)
                return;

            t=new TransportedVectorTime();
            t.senderPosition=in.readInt();
            
            int len=in.readInt();
            if (t.senderPosition<0||len<0||t.senderPosition>=len)
                throw new InstantiationException("sender position="+t.senderPosition+", values length="+len);
            
            t.values=new int[len];
            for(int i=0;i<len;i++) t.values[i]=in.readInt();
        }
        
        public String toString()
        {
            return "[CAUSALHEADER:" + t + ']';
        }
        
    }
    
    public static final class CausalNewViewHeader extends Header implements Streamable {
        
        /**
         * Serialization <code>serialVersionUID</code>.
         */
        private static final long serialVersionUID = 3257569486185183289L;
        
        public final static String NAME="CAUSAL_NEWVIEW_HEADER";
        
        /**
         * New view id.
         */
        private ViewId newViewId;
        
        /**
         * Sender local time.
         */
        private int localTime;
        
        private boolean complete;
        
        public CausalNewViewHeader(ViewId newViewId, int localTime, boolean complete) {
            this.newViewId=newViewId;
            this.localTime=localTime;
            this.complete=complete;
        }
        
        /**
         * Used for externalization.
         */
        public CausalNewViewHeader() {
        }
        
        public ViewId getNewViewId() {
            return newViewId;
        }
        
        public int getLocalTime() {
            return localTime;
        }
        
        public boolean isComplete() {
            return complete;
        }
        
        /**
         * Size of this vector timestamp estimation, used in fragmentation.
         * @return headersize in bytes
         */
        public int size() {
            /*why 231, don't know but these are this values I get when
             flattening the object into byte buffer*/
            return 231;
        }
        
        /**
         * Manual serialization
         */
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(newViewId);
            out.writeInt(localTime);
            out.writeBoolean(complete);
        }
        
        /**
         * Manual deserialization
         */
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            newViewId=(ViewId)in.readObject();
            localTime=in.readInt();
            complete=in.readBoolean();
        }
        
        public void writeTo(DataOutputStream out) throws IOException {
            newViewId.writeTo(out);
            out.writeInt(localTime);
            out.writeBoolean(complete);
        }
        
        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            newViewId=new ViewId();
            newViewId.readFrom(in);
            localTime=in.readInt();
            complete=in.readBoolean();
        }
        
        public String toString() {
            return '[' + NAME + ':' + newViewId + "; @" + localTime + (complete?"; complete]":"; incomplete]");
        }
        
    }
    
    public static final class MissingIndexesMessage implements Externalizable, Streamable {
        
        /**
         * Comment for <code>serialVersionUID</code>
         */
        private static final long serialVersionUID = 3257007644266213432L;
        
        /**
         * Member indexes the sender is waiting the local time from.
         */
        private int missingTimeIndexes[];
        
        /**
         * Member indexes the sender is waiting the completion from.
         */
        private int missingCompletionIndexes[];
        
        public MissingIndexesMessage(Collection missingLocalTimes, Collection missingCompletions) {
            missingTimeIndexes=new int[missingLocalTimes.size()];           
            int i=0;
            for(Iterator it=missingLocalTimes.iterator();it.hasNext();)
                missingTimeIndexes[i++]=((Integer)it.next()).intValue();
            
            missingCompletionIndexes=new int[missingCompletions.size()];           
            i=0;
            for(Iterator it=missingCompletions.iterator();it.hasNext();)
                missingCompletionIndexes[i++]=((Integer)it.next()).intValue();
        }
        
        /**
         * Used for externalization.
         */
        public MissingIndexesMessage() {
        }
        
        public int[] getMissingTimeIndexes() {
            return missingTimeIndexes;
        }
        
        public int[] getMissingCompletionIndexes() {
            return missingCompletionIndexes;
        }
        
        private static void writeIntArray(DataOutput out, int array[]) throws IOException {
            out.writeInt(array.length);
            for(int i=0;i<array.length;i++) out.writeInt(array[i]);
        }
        
        private static int[] readIntArray(DataInput in) throws IOException {
            int length=in.readInt();
            if (length<0) throw new IOException("array length is < 0: "+length);
            int array[]=new int[length];
            for(int i=0;i<array.length;i++) array[i]=in.readInt();
            return array;
        }
        
        public void writeExternal(ObjectOutput out) throws IOException {
            writeIntArray(out, missingTimeIndexes);
            writeIntArray(out, missingCompletionIndexes);
        }
        
        public void readExternal(ObjectInput in) throws IOException {
            missingTimeIndexes=readIntArray(in);
            missingCompletionIndexes=readIntArray(in);
        }
        
        public void writeTo(DataOutputStream out) throws IOException {
            writeIntArray(out, missingTimeIndexes);
            writeIntArray(out, missingCompletionIndexes);
        }
        
        public void readFrom(DataInputStream in) throws IOException {
            missingTimeIndexes=readIntArray(in);
            missingCompletionIndexes=readIntArray(in);
        }
        
        public String toString() {
            int missingLocalTimes[]=this.missingTimeIndexes, tmpMissingCompletionIndexes[]=this.missingCompletionIndexes;
            StringBuilder sb=new StringBuilder("MissingIndexes[");
            if (missingLocalTimes==null) {
                sb.append("? missing times - not deserialized, ");
            } else {
                sb.append(missingLocalTimes.length).append(" times missing, ");
            }
            if (tmpMissingCompletionIndexes ==null) {
                sb.append("? missing completions - not deserialized]");
            } else {
                sb.append(tmpMissingCompletionIndexes.length).append(" completions missing]");
            }
            return sb.toString();
        }
        
    }
    
    private static final class InternalView {
        
        private final ViewId viewId;
        
        private final Address members[];
        
        private final int localIndex;
        
        InternalView(ViewId viewId, List viewMembers, Address localAddress) {
            this.viewId=viewId;
            members=new Address[viewMembers.size()];
            for(int i=0;i<viewMembers.size();i++) members[i]=(Address)viewMembers.get(i);
            Arrays.sort(members);            

            int tmpLocalIndex=-1;
            
            for(int i=0;i<members.length;i++) {
                Address member=members[i];
                if (localAddress.equals(member)) tmpLocalIndex=i;
            }
            
            if (tmpLocalIndex ==-1) throw new IllegalStateException("view does not contain the local address");
            this.localIndex=tmpLocalIndex;
        }
        
        public ViewId getViewId() {
            return viewId;
        }
        
        public int getIndex(Address member) {
            return Arrays.binarySearch(members, member);
        }
        
        public Address getMember(int index) {
            return members[index];
        }
        
        public int getLocalIndex() {
            return localIndex;
        }
        
        public int size() {
            return members.length;
        }
        
        public String toString() {
            StringBuilder sb;
            sb=new StringBuilder();
            sb.append(viewId).append("; ").append(members.length).append(" members: ");
            for(int i=0;i<members.length;i++) {
                if (i>0) sb.append(", ");
                sb.append(i).append(':').append(members[i]);
            }
            sb.append(" - local is ").append(localIndex);
            return sb.toString();
        }
        
    }
    
    private final class NewCausalView {
        
        private final ActiveCausalView active;
        
        private final InternalView view;
        
        private final int timeVector[];
        
        private final TreeSet missingTimes=new TreeSet(), missingCompletions=new TreeSet();
        
        public NewCausalView(ActiveCausalView active, InternalView view) {
            this.active=active;
            this.view=view;
            
            // Setup time vector
            timeVector=new int[view.size()];

            if (view.getLocalIndex()>=timeVector.length)
                throw new IllegalStateException("View: "+view+" timevector.length="+timeVector.length);

            for(int i=0;i<view.size();i++) {
                Integer index=new Integer(i);
                missingTimes.add(index);            
                missingCompletions.add(index);            
            }
        }
        
        public InternalView getView() {
            return view;
        }
        
        public ViewId getViewId() {
            return view.getViewId();
        }
        
        public int getLocalIndex() {
            return view.getLocalIndex();
        }
        
        public synchronized int getLocalTime() {
            return timeVector[view.getLocalIndex()];
        }
        
        public synchronized void setMemberLocalTime(Address address, int time) {
            if (missingTimes.isEmpty()) return;
            
            int index=view.getIndex(address);
            if (index<0) return;
            timeVector[index]=time;           
            missingTimes.remove(new Integer(index));
            
            if (missingTimes.isEmpty()) {
                if (active!=null) active.setFinalTimeVector(view, timeVector);                        
                if (log.isTraceEnabled()) log.trace(this+" has all the times");
            }
            else {
                if (log.isTraceEnabled()) log.trace(this+" missing times: "+missingTimes);
            }
        }
        
        public synchronized void setMemberCompleted(Address address) {
            if (missingCompletions.isEmpty()) return;
            
            int index=view.getIndex(address);
            if (index<0) return;
            missingCompletions.remove(new Integer(index));
            
            if (missingCompletions.isEmpty()) {
                if (log.isTraceEnabled()) log.trace(this+" has all the completions");
            } else {
                if (log.isTraceEnabled()) log.trace(this+" missing completions: "+missingCompletions);
            }
        }
        
        public synchronized boolean hasMissingTimes() {
            return !missingTimes.isEmpty();
        }
        
        public synchronized Collection getMissingTimes() {
            return missingTimes;
        }
        
        public synchronized boolean hasMissingCompletions() {
            return !missingCompletions.isEmpty();
        }
        
        public synchronized Collection getMissingCompletions() {
            return missingCompletions;
        }
        
        public String toString() {
            return "NewCausalView["+view+']';
        }
        
    }
        
    private final class ActiveCausalView {
        
        private final InternalView view;
        
        private final int timeVector[];
        
        private int finalTimeVector[];
        
        ActiveCausalView(InternalView view, int initialTimeVector[]) {
            this.view=view;
            this.timeVector=initialTimeVector;
            finalTimeVector=null;
            
            if (view.getLocalIndex()>=initialTimeVector.length)
                throw new IllegalStateException("View: "+view+" timevector.length="+initialTimeVector.length);
        }
        
        public InternalView getView() {
            return view;
        }
        
        public ViewId getViewId() {
            return view.getViewId();
        }
        
        public int getLocalIndex() {
            return view.getLocalIndex();
        }
        
        public synchronized int getLocalTime() {
            return timeVector[view.getLocalIndex()];
        }
        
        /**
         * Increment local time.
         */
        public synchronized void increment() {
            timeVector[view.getLocalIndex()]++;
        }

        /**
         * Returns a minimal lightweight representation of this Vector Time
         * suitable for network transport.
         * @return lightweight representation of this VectorTime in the
         * form of TransportedVectorTime object
         */
        public synchronized TransportedVectorTime getTransportedVectorTime() {
            // Need to make a copy of the time vector
            int tmpTimeVector[]=new int[this.timeVector.length];
            System.arraycopy(this.timeVector, 0, tmpTimeVector, 0, tmpTimeVector.length);
            
            return new TransportedVectorTime(view.getLocalIndex(), tmpTimeVector);
        }

        public synchronized boolean isCausallyNext(TransportedVectorTime vector) {
            int senderIndex = vector.getSenderIndex();

            if (senderIndex == view.getLocalIndex()) return true;

            int[] otherTimeVector = vector.getValues();

            if (otherTimeVector.length!=timeVector.length) {
                if(log.isWarnEnabled())
                    log.warn("isCausallyNext: got message with wrong time vector length: "+otherTimeVector.length+
                            ", expected: "+timeVector.length);
                return true;
            }
            
            boolean nextCausalFromSender = false;
            boolean nextCausal = true;

            for (int i=0;i<timeVector.length;i++) {
               if ((i == senderIndex) && (otherTimeVector[i] == timeVector[i] + 1)) {
                  nextCausalFromSender = true;
                  continue;
               }
               if (i == view.getLocalIndex()) continue;
               if (otherTimeVector[i] > timeVector[i]) nextCausal = false;
            }
            
            return (nextCausalFromSender && nextCausal);
        }
        
        public synchronized void max(TransportedVectorTime vector) {
            int otherTimeVector[]=vector.getValues();
            
            if (otherTimeVector.length!=timeVector.length) {
                if(log.isWarnEnabled())
                    log.warn("max: got message with wrong time vector length: "+otherTimeVector.length+", expected: "+
                            timeVector.length);
                return;
            }
            
            for(int i=0;i<timeVector.length;i++) {
                if (otherTimeVector[i]>timeVector[i]) timeVector[i]=otherTimeVector[i];
            }
        }
        
        public synchronized void setFinalTimeVector(InternalView newView, int startTimeVector[]) {
            finalTimeVector=new int[timeVector.length];
            System.arraycopy(timeVector, 0, finalTimeVector, 0, timeVector.length);
            
            for(int i=0;i<view.size();i++) {
                Address member=view.getMember(i);
                int startIndex=newView.getIndex(member);
                if (startIndex<0) continue; // Member disappeared, keep existing time.
                finalTimeVector[i]=startTimeVector[startIndex]; // update the final time vector.
            }
            
            if (log.isTraceEnabled())
                log.trace(this+": final vector time set @ "+timeVectorString());
        }
        
        public synchronized void clearFinalTimeVector() {
            finalTimeVector=null;
        }
        
        public synchronized boolean hasEnded() {
            if (finalTimeVector==null) return false;
            for(int i=0;i<timeVector.length;i++) {
                if (timeVector[i]<finalTimeVector[i]) return false;
            }
            return true;
        }
        
        public synchronized String timeVectorString() {
            StringBuilder sb=new StringBuilder();
            for(int i=0;i<timeVector.length;i++) {
                if (i>0) sb.append(", ");
                sb.append(timeVector[i]);
            }
            return sb.toString();
        }
        
        public String toString() {
            return "ActiveCausalView["+view+']';
        }
        
    }
        
    private final class NewViewThread extends Thread {
        
        private final NewCausalView newView;
        
        private boolean updateRequested=false;
        
        NewViewThread(NewCausalView newView) {
            super(newView.getViewId().toString());
            this.newView=newView;
            setDaemon(true);
        }
        
        NewCausalView getCausalView() {
            return newView;
        }
        
        public void run() {
            boolean sendUpdate=false, complete=false;
            LinkedList flush=null;

            for(;;) {
                Message update=null;

                synchronized(lock) {
                    if(this != newViewThread) {
                        break;
                    }

                    if(newView.hasMissingTimes()) {
                        sendUpdate=true;
                    }
                    else
                    if(currentView == null || (!currentView.getViewId().equals(newView.getViewId()) && currentView.hasEnded()))
                    {
                        currentView=new ActiveCausalView(newView.getView(), newView.timeVector);
                        complete=true;
                        newView.setMemberCompleted(localAddress);

                        if(log.isTraceEnabled())
                            log.trace("Set up new active view: " + currentView + " @ " + currentView.timeVectorString());
                    }

                    if(newView.hasMissingCompletions()) {
                        sendUpdate=true;
                    }
                    else {
                        newViewThread=null;
                        enabled=true;
                        flush=(LinkedList)downwardWaitingQueue.clone();
                        downwardWaitingQueue.clear();

                        if(log.isTraceEnabled())
                            log.trace("Done synchronizing, enabled view: " + currentView + " @ " + currentView.timeVectorString());

                        break;
                    }

                    if(sendUpdate) {
                        update=new Message(null, localAddress, null);
                        update.putHeader(CausalNewViewHeader.NAME
                                , new CausalNewViewHeader(newView.getViewId(), newView.getLocalTime(), complete));
                        update.setObject(new MissingIndexesMessage(newView.getMissingTimes(), newView.getMissingCompletions()));
                    }
                }

                if(update != null) {
                    if(log.isTraceEnabled())
                        log.trace("Sending sync update");
                    down_prot.down(new Event(Event.MSG, update));
                }

                synchronized(this) {
                    // Wait for 50ms
                    try {
                        wait(500);
                    }
                    catch(InterruptedException e) {
                        Thread.currentThread().interrupt(); // set interrupt flag again
                        // Ignore
                        log.warn("Interrupted?!?", e);
                    }

                    sendUpdate=updateRequested;
                    updateRequested=false;
                }
            }
            
            if (flush!=null) {
                int n=flush.size();
                if (log.isDebugEnabled()) log.debug("Flushing "+n+" messages down...");
                
                while(!flush.isEmpty()) {
                    Event evt=(Event)flush.removeFirst();
                    down(evt);
                }
                
                if (log.isDebugEnabled()) log.debug("Done flushing "+n+" messages down...");
            }
        }
        
        void updateRequested() {
            synchronized(this) {
                updateRequested=true;
            }
        }
        
    }
    

    private final Object lock=new Object();
    
    /**
     * Local address.
     */
    private Address localAddress;
    
    /**
     * Queue containing upward messages waiting for delivery i.e causal order.
     */
    private final LinkedList upwardWaitingQueue=new LinkedList();
    
    /**
     * Queue containing downward messages waiting for sending.
     */
    private final LinkedList downwardWaitingQueue=new LinkedList();
    
    private boolean enabled=false;
    
    /**
     * The active view (including its time vector), if any.
     */
    private ActiveCausalView currentView;
    
    private NewViewThread newViewThread;
    
    private boolean debug=false;
    
    /**
     * Default constructor.
     */
    public CAUSAL() {
    }
    
    public boolean setProperties(Properties props) {
        if (!super.setProperties(props)) return false;
        
        String s=props.getProperty("debug");
        debug="debug".equalsIgnoreCase(s);
        
        return true;
    }
    
    /**
     * Adds a vectortimestamp to a sorted queue
     * @param tvt A vector time stamp
     */
    private void addToDelayQueue(TransportedVectorTime tvt)
    {
        ListIterator i = upwardWaitingQueue.listIterator(0);
        TransportedVectorTime current = null;
        while (i.hasNext())
        {
            current = (TransportedVectorTime) i.next();
            if (tvt.lessThanOrEqual(current))
            {
                upwardWaitingQueue.add(i.previousIndex(), tvt);
                return;
            }
        }
        upwardWaitingQueue.add(tvt);
    }
    
    // Must be called sync'd on lock
    private void disable() {
        enabled=false;
    }
    
    // Must be called sync'd on lock
    private boolean isEnabled() {
        return enabled;
    }
    
//    // Must be called sync'd on lock
//    private boolean tryEnable() {
//        if (currentView!=null && !currentView.hasEnded()) return false;
//        
//        currentView=new ActiveCausalView(newView.getView(), newView.timeVector);
//        newView=null;
//        enabled=true;
//        
//        // FIXME send all waiting messages.
//        
//        return true;
//    }
    
    /**
     * Process a downward event.
     * @param evt The event.
     */
    public Object down(Event evt) {
        try {
            // If not a MSG, just pass down.
            if (evt.getType()!=Event.MSG) {
                return down_prot.down(evt);
            }
            
            Message msg = (Message) evt.getArg();
            
            // If unicast, just pass down.
            if (msg.getDest()!=null && ! msg.getDest().isMulticastAddress()) {
                return down_prot.down(evt);
            }
    
            // Multicast MSG:
            // - if enabled, get the next time vector, add it and pass down;
            // - otherwise, add to the downward waiting queue.
            TransportedVectorTime tvt=null;
            
            synchronized(lock) {
                if (isEnabled()) {
                    currentView.increment();
                    tvt=currentView.getTransportedVectorTime();
                    if (log.isTraceEnabled()) log.trace("Sent 1 down message @ "+currentView.timeVectorString());
                } else {
                    if (log.isTraceEnabled()) log.trace("Enqueued 1 down message...");
                    downwardWaitingQueue.add(evt);
                }
            }
            
            if (tvt!=null) {
                msg.putHeader(getName(), new CausalHeader(tvt));
                return down_prot.down(evt);
            }
        } catch (RuntimeException e) {
            if (debug) log.error("*** down: "+e.getMessage(), e);
            throw e;
        }

        return null;
    }
    
    /**
     * Process an upward event.
     * @param evt The event.
     */
    public Object up(Event evt) {
        try {
            switch (evt.getType()) {
                case Event.SET_LOCAL_ADDRESS:
                    upSetLocalAddress(evt);
                    break;               
                case Event.VIEW_CHANGE:
                    upViewChange(evt);
                    break;                    
                case Event.MSG:
                    return upMsg(evt);
                default:
                    return up_prot.up(evt);
            }
        } catch (RuntimeException e) {
            if (debug) log.error("*** up: "+e.getMessage(), e);
            throw e;
        }
        return null;
    }
    
    private void upSetLocalAddress(Event evt) {
        localAddress = (Address) evt.getArg();
        up_prot.up(evt);
    }
   
    /**
     * Process a VIEW_CHANGE event.
     * @param evt The event.
     */
    private void upViewChange(Event evt)
    {
        View view=(View)evt.getArg();      
        InternalView iView=new InternalView(view.getVid(), view.getMembers(), localAddress);
        if(log.isDebugEnabled())
            log.debug("New view: "+view);
        
        synchronized(lock) {
            // Disable sending
            disable();
            
            // Create new causal view
            NewCausalView newView=new NewCausalView(currentView, iView);
            if (currentView!=null) {
                currentView.clearFinalTimeVector();
                newView.setMemberLocalTime(localAddress, currentView.getLocalTime());
            } else {
                newView.setMemberLocalTime(localAddress, 0);
            }
            
            if (log.isTraceEnabled()) log.trace("Starting synchronization thread for "+newView);
            
            newViewThread=new NewViewThread(newView);
            newViewThread.start();
        }
        
        up_prot.up(evt);
    }
    
    private Object upMsg(Event evt) {
        Message msg = (Message) evt.getArg();
        Address src=msg.getSrc();
        
        // Check for a causal new view header
        Object obj = msg.getHeader(CausalNewViewHeader.NAME);

        if (obj instanceof CausalNewViewHeader) {
            processNewViewSynchronization(src, (CausalNewViewHeader)obj, msg.getObject());           
            return null;
        }
        
        obj = msg.getHeader(getName());

        if (!(obj instanceof CausalHeader)) {
            if((msg.getDest() == null || msg.getDest().isMulticastAddress()) 
                    && log.isErrorEnabled()) log.error("NO CAUSAL.Header found");
            return up_prot.up(evt);
        }
        
        TransportedVectorTime messageVector = ((CausalHeader)obj).getVectorTime();
        
        synchronized (lock) {
            if (currentView==null||currentView.getView().getIndex(src)<0) {
                if (log.isDebugEnabled()) log.debug("Discarding "+obj+" from "+msg.getSrc());
                return null;
            }
            
            if (currentView.isCausallyNext(messageVector)) {
                if (log.isTraceEnabled()) log.trace("passing up message "+msg+", headers are "+msg.printHeaders()+", local vector is "+currentView.timeVectorString());
                up_prot.up(evt);
                currentView.max(messageVector);
            } else  {
                if (log.isTraceEnabled()) log.trace("queuing message "+msg+", headers are "+msg.printHeaders());
                messageVector.setAssociatedMessage(msg);
                addToDelayQueue(messageVector);
            }
            
            TransportedVectorTime queuedVector = null;
            
            while ((!upwardWaitingQueue.isEmpty()) &&
                    currentView.isCausallyNext((queuedVector = (TransportedVectorTime) upwardWaitingQueue.getFirst()))) {
                upwardWaitingQueue.remove(queuedVector);
                Message tmp=queuedVector.getAssociatedMessage();
                if (log.isTraceEnabled()) log.trace("released message "+tmp+", headers are "+tmp.printHeaders());
                up_prot.up(new Event(Event.MSG, tmp));
                currentView.max(queuedVector);
            }
        }
        return null;
    }
    
    /**
     * 
     */
    private void processNewViewSynchronization(Address src, CausalNewViewHeader header, Object object) {
        // If from ourselves, ignore.
        if (localAddress.equals(src)) return;
        
        MissingIndexesMessage content=(MissingIndexesMessage)object;

        if(log.isTraceEnabled())
            log.trace("Got sync update from "+src);
        
        synchronized(lock) {
            if (newViewThread==null) {
                if (currentView!=null&&currentView.getView().getViewId().equals(header.newViewId)) {
                    // Somebody's late...
                    int localIndex=currentView.getLocalIndex();
                    
                    if (Arrays.binarySearch(content.getMissingCompletionIndexes(), localIndex)>=0) {
                        Message update=new Message(null, localAddress, null);
                        update.putHeader(CausalNewViewHeader.NAME
                                , new CausalNewViewHeader(currentView.getView().getViewId(), 0, true)); // It has the time already
                        update.setObject(new MissingIndexesMessage(Collections.EMPTY_LIST, Collections.EMPTY_LIST));
                        
                        down_prot.down(new Event(Event.MSG, update));
                    }
                    
                    if (Arrays.binarySearch(content.getMissingTimeIndexes(), localIndex)>=0) {
                        
                    }
                } else {
                    // Somebody's early...
                    disable();
                }
                return;
            }
            
            if (!newViewThread.getCausalView().getViewId().equals(header.newViewId)) return;
        
            if (log.isTraceEnabled()) log.trace("From "+src+": "+header);

            // Update the local time and completion status for the source.
            newViewThread.getCausalView().setMemberLocalTime(src, header.localTime);
            if (header.isComplete()) newViewThread.getCausalView().setMemberCompleted(src);
            
            // Check the requested times and completions
            int localIndex=newViewThread.getCausalView().getLocalIndex();
            
            if ( Arrays.binarySearch(content.getMissingTimeIndexes(), localIndex)>=0
                    || Arrays.binarySearch(content.getMissingCompletionIndexes(), localIndex)>=0 ) {
                newViewThread.updateRequested();
            }
        }      
    }

    /**
     * Returns a name of this stack, each stackhas to have unique name
     * @return stack's name - CAUSAL
     */
    public String getName() {
        return "CAUSAL";
    }
    
}

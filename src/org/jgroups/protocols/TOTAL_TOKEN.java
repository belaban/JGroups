//$Id: TOTAL_TOKEN.java,v 1.4 2004/03/30 06:47:21 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.protocols.pbcast.Digest;
import org.jgroups.protocols.ring.RingNodeFlowControl;
import org.jgroups.protocols.ring.RingToken;
import org.jgroups.protocols.ring.TokenLostException;
import org.jgroups.protocols.ring.UdpRingNode;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.RpcProtocol;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

/**
 * <p>
 * Total order implementation based on <a href="http://citeseer.nj.nec.com/amir95totem.html">
 * The Totem Single-Ring Ordering and Membership Protocol</a>
 * <p>
 *
 * <p>
 * However, this is an adaption of algorithm mentioned in the research paper above since we reuse
 * our own membership protocol and failure detectors. Somewhat different flow control mechanism is
 * also implemented.
 *
 * <p>
 * Token passing is done through reliable point-to-point udp channels provided by UNICAST layer.
 * Process groups nodes members are organized in a logical ring.
 * </p>
 *
 * <p>
 * Total token layer doesn't need NAKACK nor STABLE layer beneath it since it implements it's own
 * retransmission and tracks stability of the messages from the information piggybacked on the
 * token itself.
 * </p>
 *
 * <p>
 * For the typical protocol stack configuration used, see org.jgroups.demos.TotalTokenDemo and
 * total-token.xml configuration file provided with this distribution of JGroups.
 * </p>
 *
 *
 *
 *@author Vladimir Blagojevic vladimir@cs.yorku.ca
 *@version $Revision: 1.4 $
 *
 *@see org.jgroups.protocols.ring.RingNodeFlowControl
 *@see org.jgroups.protocols.ring.RingNode
 *@see org.jgroups.protocols.ring.TcpRingNode
 *@see org.jgroups.protocols.ring.UdpRingNode
 *
 **/


public class TOTAL_TOKEN extends RpcProtocol
{


   public static class TotalTokenHeader extends Header
   {


      /**
       * sequence number of the message
       */
      private long seq;


      /**
       *used for externalization
       */
      public TotalTokenHeader()
      {
      }

      public TotalTokenHeader(long seq)
      {
         this.seq = seq;
      }

      public TotalTokenHeader(Long seq)
      {
         this.seq = seq.longValue();
      }


      /**
       *Returns sequence number of the message that owns this header
       *@return sequence number
       */
      public long getSeq()
      {
         return seq;
      }

      /**
       *Returns size of the header
       * @return headersize in bytes
       */
      public long size()
      {
         //calculated using Util.SizeOf(Object)
         return 121;
      }

      /**
       * Manual serialization
       *
       *
       */
      public void writeExternal(ObjectOutput out) throws IOException
      {
         out.writeLong(seq);
      }

      /**
       * Manual deserialization
       *
       */
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
      {
         seq = in.readLong();
      }

      public String toString()
      {
         return "[TotalTokenHeader=" + seq + "]";
      }
   }

   public static class RingTokenHeader extends Header
   {
      public RingTokenHeader()
      {
      }

      public void writeExternal(ObjectOutput out) throws IOException
      {
      }

      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
      {
      }

      public long size()
      {
         //calculated using Util.SizeOf(Object)
         return 110;
      }
   }


   private static int OPERATIONAL_STATE = 0;
   private static int RECOVERY_STATE = 1;

   UdpRingNode node;
   RingNodeFlowControl flowControl;
   Address localAddress;
   TokenTransmitter tokenRetransmitter=new TokenTransmitter();
   List newMessagesQueue;
   SortedSet liveMembersInRecovery,suspects;

   Object mutex = new Object();
   TreeMap receivedMessagesQueue;
   long myAru = 0;

   Object threadCoordinationMutex = new Object();
   boolean tokenInStack = false;
   boolean threadDeliveringMessage = false;
   boolean tokenSeen = false;


   volatile boolean isRecoveryLeader = false;
   volatile int state;
   volatile int sleepTime = 10;

   long highestSeenSeq = 0;
   long lastRoundTokensAru = 0;
   int lastRoundTransmitCount,lastRoundRebroadcastCount = 0;
   int blockSendingBacklogThreshold = Integer.MAX_VALUE;
   int unblockSendingBacklogThreshold = Integer.MIN_VALUE;
   boolean tokenCirculating = false;
   boolean senderBlocked = false;
   public static final String prot_name = "TOTAL_TOKEN";


   public String getName()
   {
      return prot_name;
   }

   private String getState()
   {
      if (state == OPERATIONAL_STATE)
      {
         return "OPERATIONAL";
      }
      else
         return "RECOVERY";
   }


    public void start() throws Exception {
        super.start();
        newMessagesQueue = Collections.synchronizedList(new ArrayList());
        receivedMessagesQueue = new TreeMap();
        tokenRetransmitter.start();
    }

   /**
    * Overrides @org.jgroups.stack.MessageProtocol#stop().
    */
   public void stop()
   {
       super.stop();
       tokenRetransmitter.shutDown();
   }




   /**
    * Setup the Protocol instance acording to the configuration string
    *
    */
   public boolean setProperties(Properties props)
   {
      String str;
      str = props.getProperty("block_sending");
      if (str != null)
      {
         blockSendingBacklogThreshold = Integer.parseInt(str);
         props.remove("block_sending");
      }

      str = props.getProperty("unblock_sending");
      if (str != null)
      {
         unblockSendingBacklogThreshold = Integer.parseInt(str);
         props.remove("unblock_sending");
      }

      if (props.size() > 0)
      {
         System.err.println("UDP.setProperties(): the following properties are not recognized:");
         props.list(System.out);
         return false;
      }
      return true;
   }

   public IpAddress getTokenReceiverAddress()
   {
      return node != null? node.getTokenReceiverAddress() : null;
   }

   public Vector providedUpServices()
   {
      Vector retval = new Vector();
      retval.addElement(new Integer(Event.GET_DIGEST));
      retval.addElement(new Integer(Event.GET_DIGEST_STATE));
      retval.addElement(new Integer(Event.SET_DIGEST));
      return retval;
   }

   public boolean handleUpEvent(Event evt)
   {
      Message msg;
      Header h;
      switch (evt.getType())
      {

         case Event.SET_LOCAL_ADDRESS:
            localAddress = (Address) evt.getArg();
            node = new UdpRingNode(this, localAddress);
            flowControl = new RingNodeFlowControl();
            break;

         case Event.SUSPECT:
            Address suspect = (Address) evt.getArg();
            onSuspectMessage(suspect);
            break;

         case Event.MSG:
            msg = (Message) evt.getArg();
            h = msg.getHeader(getName());
            if (h instanceof TotalTokenHeader)
            {
               messageArrived(msg);
               return false;
            }
            else if (h instanceof RingTokenHeader)
            {
                if(node != null) {
                    Object tmp=msg.getObject();
                    node.tokenArrived(tmp);
                }
               return false;
            }
      }
      return true;
   }

   public boolean handleDownEvent(Event evt)
   {
      switch (evt.getType())
      {
         case Event.GET_DIGEST:
         case Event.GET_DIGEST_STATE:

            Digest d = new Digest(members.size());
            Address sender = null;
            //all members have same digest :)
            for (int j = 0; j < members.size(); j++)
            {
               sender = (Address) members.elementAt(j);
               d.add(sender, highestSeenSeq, highestSeenSeq);
            }
            passUp(new Event(Event.GET_DIGEST_OK, d));
            return false;
         case Event.SET_DIGEST:
            Digest receivedDigest = (Digest) evt.getArg();
            myAru = receivedDigest.highSeqnoAt(0);
            return false;

         case Event.VIEW_CHANGE:
            onViewChange();
            return true;

            /*
         case Event.CLEANUP:
            // do not pass cleanup event
            //further down. This is a hack to enable
            // sucessfull leave from group when using pbcast.GMS.
            // It just buys us 5 seconds to imminent STOP
            // event following CLEANUP. We hope that the moment
            // this node disconnect up until new view is installed
            // at other members is less than 5 seconds.

            //The proper way would be to:
            //trap DISCONNECT event on the way down, do not pass it further.
            //wait for the new view to be installed (effectively excluding this node out of
            //ring) , wait for one token roundtrip time, and then send that trapped
            //DISCONNECT event down furhter to generate DISCONNECT_OK on the way up.
            // CLEANUP and STOP are generated after DISCONNECT.

            //However, as the things stand right now pbcast.GMS stops working immediately
            //when it receives DISCONNECT thus the new view is never generated in node that is
            //leaving the group.

            //pbcsat.GMS should still generate new view and stop working when
            //it receives STOP event.

            //In timeline DISCONNECT < CLEANUP < STOP
            return false;
            */

         case Event.MSG:
            Message msg = (Message) evt.getArg();
            //handle only multicasts
            if (msg == null) return false;
            if (msg.getDest() == null || msg.getDest().isMulticastAddress())
            {
               newMessagesQueue.add(msg);
               return false;
            }
      }
      return true;
   }

   private void onViewChange()
   {
      isRecoveryLeader = false;

      if (suspects != null)
      {
         suspects.clear();
         suspects = null;
      }
      if (liveMembersInRecovery != null)
      {
         liveMembersInRecovery.clear();
         liveMembersInRecovery = null;
      }
   }

   private void onSuspectMessage(Address suspect)
   {
      state = RECOVERY_STATE;
      if (suspects == null || suspects.size() == 0)
      {
         suspects = Collections.synchronizedSortedSet(new TreeSet());
         liveMembersInRecovery = Collections.synchronizedSortedSet(new TreeSet(members));
      }
      suspects.add(suspect);
      liveMembersInRecovery.removeAll(suspects);
      isRecoveryLeader = isRecoveryLeader(liveMembersInRecovery);
   }

   /**
    * Given a set of surviving members in the transitioanl view
    * returns true if this stack is elected to be recovery leader.
    *
    */
   private boolean isRecoveryLeader(SortedSet liveMembers)
   {
      boolean recoveryLeader = false;
      if (liveMembers.size() > 0)
      {
         recoveryLeader = localAddress.equals(liveMembers.first());
      }

      {
         if(log.isInfoEnabled()) log.info("live memebers are " + liveMembers);
         if(log.isInfoEnabled()) log.info("I am recovery leader?" + recoveryLeader);
      }
      return recoveryLeader;

   }

   public long getAllReceivedUpTo()
   {
      return myAru;
   }

   public void installTransitionalView(Vector members)
   {
       if(node != null)
           node.reconfigure(members);
   }

   /**
    *  Total Token recovery protocol (TTRP)
    *
    *
    *
    *  Upon transition to recovery state, coordinator sends multiple reliable
    *  unicasts message requesting each ring member to report it's allReceivedUpto
    *  value. When all replies are received, a response list of allReceivedUpto
    *  values is sorted and transformed into a set while dropping the lowest
    *  allReceivedUpto value. For example , received response list  [4,4,5,6,7,7,8]
    *  is transformed into [5,6,7,8] thus not including the lowest value 4.
    *
    *  The objective of the recovery protocol is to have each member receive all
    *  messages up to maximum sequence value M from the response list, thus
    *  satisfying virtual synchrony properties.
    *
    *  Note that if all members report the same allReceivedUpto values, then
    *  virtual synchrony is satisfied (since all surviving members have seen
    *  the same set of messages) and we can immediately inject operational
    *  token which will enable installment of the new view.
    *
    *  Otherwise, a constructed set S of all allReceivedUpto values represent sequence ids
    *  of messages that have to be received by all mebers prior to installing new
    *  view thus satisfying virtual synchrony properties.
    *
    *  A transitional view, visible only to TOTAL_TOKEN layer is then installed
    *  on the ring by a coordinator. Again a multiple unicast are used. A
    *  transitional view is deduced from current view by excluding suspected members.
    *  Coordinator creates a recovery token by appending the set S of sequence ids to
    *  token retransmission request list. Recovery token is then inserted into
    *  transitional ring view.
    *
    *  Upon reception of recovery token, ring members are not allowed to transmit
    *  any additional new messages but only to retransmit messages from the
    *  specified token retransmission request list.
    *
    *  When all member detect that they have received all messages upto sequence
    *  value M , the next member that first receives token converts it to operatioanl
    *  token and  normal operational state is restored in all nodes.
    *
    *  If token is lost during recovery stage, recovery protocol is restarted.
    *
    * */
   private void recover()
   {

      if (isRecoveryLeader && state == RECOVERY_STATE)
      {

         {
            if(log.isInfoEnabled()) log.info("I am starting recovery now");
         }

         Vector m = new Vector(liveMembersInRecovery);

          RspList list=callRemoteMethods(m, "getAllReceivedUpTo", new Object[]{}, new Class[]{}, GroupRequest.GET_ALL, 0);
         //RspList list = callRemoteMethods(m, "getAllReceivedUpTo", GroupRequest.GET_ALL, 0);
         Vector myAllReceivedUpTos = list.getResults();

          callRemoteMethods(m, "getAllReceivedUpTo", new Object[]{}, new Class[]{}, GroupRequest.GET_ALL, 0);
         //callRemoteMethods(m, "getAllReceivedUpTo", GroupRequest.GET_ALL, 0);
         Vector myAllReceivedUpTosConfirm = list.getResults();


         while (!myAllReceivedUpTos.equals(myAllReceivedUpTosConfirm))
         {
            myAllReceivedUpTos = myAllReceivedUpTosConfirm;
             callRemoteMethods(m, "getAllReceivedUpTo", new Object[]{}, new Class[]{}, GroupRequest.GET_ALL, 0);
            // callRemoteMethods(m, "getAllReceivedUpTo", GroupRequest.GET_ALL, 0);
            myAllReceivedUpTosConfirm = list.getResults();

            {
               if(log.isInfoEnabled()) log.info("myAllReceivedUpto values are" + myAllReceivedUpTos);
               if(log.isInfoEnabled()) log.info("myAllReceivedUpto confirm values are " + myAllReceivedUpTosConfirm);
            }
         }


         {
            if(log.isInfoEnabled()) log.info("myAllReceivedUpto stabilized values are" + myAllReceivedUpTos);
            if(log.isInfoEnabled()) log.info("installing transitional view to repair the ring...");
         }

          callRemoteMethods(m, "installTransitionalView", new Object[]{m}, new String[]{Vector.class.getName()},
                            GroupRequest.GET_ALL, 0);
         //callRemoteMethods(m, "installTransitionalView", m, GroupRequest.GET_ALL, 0);

         Vector xmits = prepareRecoveryRetransmissionList(myAllReceivedUpTos);
         RingToken injectToken = null;
         if (xmits.size() > 1)
         {

            {
               if(log.isInfoEnabled()) log.info("VS not satisfied, injecting recovery token...");
            }
            long aru = ((Long) xmits.firstElement()).longValue();
            long highest = ((Long) xmits.lastElement()).longValue();

            injectToken = new RingToken(RingToken.RECOVERY);
            injectToken.setHighestSequence(highest);
            injectToken.setAllReceivedUpto(aru);


            Collection rtr = injectToken.getRetransmissionRequests();
            rtr.addAll(xmits);
         }
         else
         {

            {
               if(log.isInfoEnabled()) log.info("VS satisfied, injecting operational token...");
            }
            injectToken = new RingToken();
            long sequence = ((Long) xmits.firstElement()).longValue();
            injectToken.setHighestSequence(sequence);
            injectToken.setAllReceivedUpto(sequence);
         }
          if(node != null)
              node.passToken(injectToken);
         tokenRetransmitter.resetTimeout();
      }
   }

   /**
    * Prepares a retransmissions list for recovery protocol
    * given a collection of all myReceivedUpTo values as
    * reported by polled surviving members.
    *
    *
    *
    */
   private Vector prepareRecoveryRetransmissionList(Vector sequences)
   {
      Collections.sort(sequences);
      Long first = (Long) sequences.firstElement();
      Long last = (Long) sequences.lastElement();


      Vector retransmissions = new Vector();
      if (first.equals(last))
      {
         retransmissions.add(new Long(first.longValue()));
      }
      else
      {
         for (long j = first.longValue() + 1; j <= last.longValue(); j++)
         {
            retransmissions.add(new Long(j));
         }
      }
      return retransmissions;
   }


   protected void updateView(View newMembers)
   {
      super.updateView(newMembers);
      Vector newViewMembers = newMembers.getMembers();
      flowControl.viewChanged(newViewMembers.size());
       if(node != null)
           node.reconfigure(newViewMembers);
      boolean isCoordinator = localAddress.equals(newViewMembers.firstElement());
      int memberSize = newViewMembers.size();

      if (memberSize == 1 && isCoordinator && !tokenCirculating)
      {
         //create token for the first time , lets roll
         tokenCirculating = true;
         RingToken token = new RingToken();
          if(node != null)
              node.passToken(token);
         tokenRetransmitter.resetTimeout();
      }
      sleepTime = (20/memberSize);
   }


   /**
    * TOTAL_TOKEN's up-handler thread invokes this method after multicast
    * message originating from some other TOTAL_TOKEN stack layer arrives at
    * this stack layer.
    *
    * Up-handler thread coordinates it's access to a shared variables
    * with TokenTransmitter thread.
    *
    * See tokenReceived() for details.
    *
    */
   private void messageArrived(Message m)
   {
      TotalTokenHeader h = (TotalTokenHeader) m.getHeader(getName());
      long seq = h.getSeq();


      synchronized (mutex)
      {
         if ((myAru + 1) <= seq)
         {
            if (seq > highestSeenSeq)
            {
               highestSeenSeq = seq;
            }

            receivedMessagesQueue.put(new Long(seq), m);
            if ((myAru + 1) == seq)
            {
               myAru = seq;
               passUp(new Event(Event.MSG, m));
            }
            if (isReceiveQueueHolePlugged())
            {
               myAru = deliverMissingMessages();
            }
         }
      }
   }

   /**
    * Returns true if there is a hole in receive queue and at
    * least one messages with sequence id consecutive to myAru.
    *
    *
    */
   private boolean isReceiveQueueHolePlugged()
   {
      return ((myAru < highestSeenSeq) && receivedMessagesQueue.containsKey(new Long(myAru + 1)));
   }


   /**
    * Delivers as much as possible messages from receive
    * message queue as long as they are consecutive with
    * respect to their sequence ids.
    *
    */
   private long deliverMissingMessages()
   {
      Map.Entry entry = null;
      boolean inOrder = true;
      long lastDelivered = myAru;
      Set deliverySet = receivedMessagesQueue.tailMap(new Long(myAru + 1)).entrySet();


      {
         if(log.isInfoEnabled()) log.info("hole getting plugged, prior muAru " + myAru);
      }


      for (Iterator iterator = deliverySet.iterator();inOrder && iterator.hasNext();)
      {
         entry = (Map.Entry) iterator.next();
         long nextInQueue = ((Long) entry.getKey()).longValue();
         if (lastDelivered + 1 == nextInQueue)
         {
            Message m = (Message) entry.getValue();
            passUp(new Event(Event.MSG, m));
            lastDelivered++;
         }
         else
         {
            inOrder = false;
         }
      }


      {
         if(log.isInfoEnabled()) log.info("hole getting plugged, post muAru " + lastDelivered);
      }
      return lastDelivered;
   }

   /**
    * Checks if the receivedMessageQueue has any missing sequence
    * numbers in it, and if it does it finds holes in sequences from
    * this stack's receivedMessageQueue and adds them to token retransmission
    * list, thus informing other group members about messages missing
    * from this stack.
    *
    *
    */
   private void updateTokenRtR(RingToken token)
   {
      long holeLowerBound = 0;
      long holeUpperBound = 0;
      Long missingSequence = null;
      Collection retransmissionList = null;


      //any holes?
      if (myAru < token.getHighestSequence())
      {
         retransmissionList = token.getRetransmissionRequests();
         Set received = receivedMessagesQueue.tailMap(new Long(myAru + 1)).keySet();
         Iterator nonMissing = received.iterator();
         holeLowerBound = myAru;


            if(log.isDebugEnabled()) log.debug("retransmission request prior" + retransmissionList);

         while (nonMissing.hasNext())
         {
            Long seq = (Long) nonMissing.next();
            holeUpperBound = seq.longValue();

            while (holeLowerBound < holeUpperBound)
            {
               missingSequence = new Long(++holeLowerBound);
               retransmissionList.add(missingSequence);
            }
            holeLowerBound = holeUpperBound;
         }

         holeUpperBound = token.getHighestSequence();
         while (holeLowerBound < holeUpperBound)
         {
            missingSequence = new Long(++holeLowerBound);
            retransmissionList.add(missingSequence);
         }


            if(log.isDebugEnabled()) log.debug("retransmission request after" + retransmissionList);
      }
   }


   /**
    * Sends messages in this stacks's outgoing queue and
    * saves a copy of each outgoing message in case they got lost.
    * If messages get lost it is thus guaranteed that each stack
    * that sent any message has a copy of it ready for retransmitting.
    *
    * Each sent message is stamped by monotonically increasing
    * sequence number starting from the highest sequence "seen"
    * on the ring.
    *
    * Returns number of messages actually sent.  The number of
    * sent messages is bounded above by the flow control
    * algorithm (allowedCount) and bounded below by the number
    * of pending messages in newMessagesQueue.
    */
   private int broadcastMessages(int allowedCount, RingToken token)
   {
      List sendList = null;
      synchronized (newMessagesQueue)
      {
         int queueSize = newMessagesQueue.size();

         if (queueSize <= 0)
         {
            return 0;
         }

         else if (queueSize > allowedCount)
         {
            sendList = new ArrayList(newMessagesQueue.subList(0, allowedCount));
            newMessagesQueue.removeAll(sendList);
         }

         else
         {
            sendList = new ArrayList();
            sendList.addAll(newMessagesQueue);
            newMessagesQueue.clear();
         }
      }

      long tokenSeq = token.getHighestSequence();

      for (Iterator iterator = sendList.iterator(); iterator.hasNext();)
      {
         Message m = (Message) iterator.next();
         m.setSrc(localAddress);
         m.setDest(null); // mcast address
         m.putHeader(getName(), new TotalTokenHeader(++tokenSeq));
         receivedMessagesQueue.put(new Long(tokenSeq), m);
         passDown(new Event(Event.MSG, m));
      }

      if (token.getHighestSequence() == token.getAllReceivedUpto())
      {
         token.setAllReceivedUpto(tokenSeq);
      }
      token.setHighestSequence(tokenSeq);
      return sendList.size();
   }


   /**
    * TokenTransmitter thread runs this method after receiving token.
    * Thread is possibly blocked if up-handler thread is currently running
    * through this stack i.e delivering an Event. Up-hanlder thread will
    * notify blocked TokenTransmitter thread when it has delivered current
    * Event so TokenTransmitter can proceed.
    * TokenTransmitter thread in turn notifies possibly blocked up-handler thread
    * when token has left the stack. Thus TokenTransmitter and up-hadler thread
    * coordinate their access to shared variables(receivedMessageQueue and myAru).
    *
    * tokenReceived method and subsequent methods called from tokenReceived represent
    * in some parts the totaly ordered algorithm presented in Amir's paper (see class
    * header for link)
    *
    *
    *
    */
   private void tokenReceived(RingToken token)
   {


      {
         if(log.isInfoEnabled()) log.info(token.toString());
         if(log.isDebugEnabled()) log.debug(getState());
      }


      flowControl.setBacklog(newMessagesQueue.size());
      flowControl.updateWindow(token);


      blockSenderIfRequired();
      unBlockSenderIfAcceptable();


      long tokensAru = 0;
      int broadcastCount = 0;
      int rebroadcastCount = 0;
      synchronized (mutex)
      {
         if (!tokenSeen)
         {
            long lastRoundAru = token.getHighestSequence() - token.getLastRoundBroadcastCount();
            if (myAru < token.getAllReceivedUpto())
            {
               myAru = lastRoundAru;
            }
            //if(log.isInfoEnabled()) log.info("TOTAL_TOKEN.tokenReceived()", "tokenSeen " + myAru);
            tokenSeen = true;
         }

         if (token.getType() == RingToken.RECOVERY)
         {
            highestSeenSeq = token.getHighestSequence();
            if (highestSeenSeq == myAru)
            {
               if(log.isInfoEnabled()) log.info("member node recovered");
               token.addRecoveredMember(localAddress);
            }
         }

         updateTokenRtR(token);

         int allowedToBroadcast = flowControl.getAllowedToBroadcast(token);
         rebroadcastCount = rebroadcastMessages(token);
         allowedToBroadcast -= rebroadcastCount;


         {
            if(log.isInfoEnabled()) log.info("myAllReceivedUpto" + myAru);
            if(log.isInfoEnabled()) log.info("allowedToBroadcast" + allowedToBroadcast);
            if(log.isInfoEnabled()) log.info("newMessagesQueue.size()" + newMessagesQueue.size());
         }

         tokensAru = token.getAllReceivedUpto();

         if (myAru < tokensAru ||
                 localAddress.equals(token.getAruId()) ||
                 token.getAruId() == null)
         {
            token.setAllReceivedUpto(myAru);
            if (token.getAllReceivedUpto() == token.getHighestSequence())
            {
               token.setAruId(null);
            }
            else
            {
               token.setAruId(localAddress);
            }
         }
         if (allowedToBroadcast > 0 && token.getType() == RingToken.OPERATIONAL)
         {
            broadcastCount = broadcastMessages(allowedToBroadcast, token);
         }

         if (tokensAru > lastRoundTokensAru)
         {
            removeStableMessages(receivedMessagesQueue, lastRoundTokensAru);
         }

      } //end synchronized

      //give CPU some breath
      Util.sleep(sleepTime);

      token.incrementTokenSequence();
      token.addLastRoundBroadcastCount(broadcastCount - lastRoundTransmitCount);
      token.addBacklog(flowControl.getBacklogDifference());
      flowControl.setPreviousBacklog();
      lastRoundTransmitCount = broadcastCount;
      lastRoundRebroadcastCount = rebroadcastCount;
      lastRoundTokensAru = tokensAru;
   }

   /**
    *
    * Rebroadcasts messages specified in token's retransmission
    * request list if those messages are available in this stack.
    * Returns number of rebroadcasted messages.
    *
    */
   private int rebroadcastMessages(RingToken token)
   {
      int rebroadCastCount = 0;
      Collection rexmitRequests = token.getRetransmissionRequests();
      if (rexmitRequests.size() > 0)
      {
         Collection rbl = getRebroadcastList(rexmitRequests);
         rebroadCastCount = rbl.size();
         if (rebroadCastCount > 0)
         {

            {
               if(log.isInfoEnabled()) log.info("rebroadcasting " + rbl);
            }

            Long s = null;
            for (Iterator iterator = rbl.iterator(); iterator.hasNext();)
            {
               s = (Long) iterator.next();
               Message m = (Message) receivedMessagesQueue.get(s);
               passDown(new Event(Event.MSG, m));
            }
         }
      }
      return rebroadCastCount;
   }


   private void invalidateOnTokenloss()
   {
      lastRoundTransmitCount = 0;
      flowControl.invalidate();
   }

   /**
    * Checks if the down pending queue's (newMessagesQueue) size is
    * greater than blockSendingBacklogThreshold specified in the properties.
    * If it is, client's sending thread is effectively blocked until
    * down pending queue's size drops below unblockSendingBacklogThreshold.
    *
    *
    */
   private void blockSenderIfRequired()
   {
      if (!senderBlocked && flowControl.getBacklog() > blockSendingBacklogThreshold)
      {
         passUp(new Event(Event.BLOCK_SEND));
         senderBlocked = true;
      }
   }

   /**
    * Checks if the down pending queue's (newMessagesQueue) size is
    * smaller than unblockSendingBacklogThreshold specified in the properties.
    * If it is, client's sending thread is effectively unblocked enabling
    * new messages to be queued for transmission.
    *
    *
    */
   private void unBlockSenderIfAcceptable()
   {
      if (senderBlocked && flowControl.getBacklog() < unblockSendingBacklogThreshold)
      {
         passUp(new Event(Event.UNBLOCK_SEND));
         senderBlocked = false;
      }

   }

   /**
    * Removes messages determined to be stable(i.e seen by all members)
    * from the specified queue.  If the client also clears all reference
    * to these messages (in application space) they become eligible for garabge
    * collection.
    *
    *
    */

   private void removeStableMessages(TreeMap m, long upToSeq)
   {

      if (m.size() > 0)
      {
         long first = ((Long) m.firstKey()).longValue();
         if (first > upToSeq)
         {
            upToSeq = first;
         }


         {
            if(log.isDebugEnabled()) log.debug("cutting queue first key " + m.firstKey()
                        + " cut at " + upToSeq + " last key " + m.lastKey());
         }
         SortedMap stable = m.headMap(new Long(upToSeq));
         stable.clear();
      }
   }

   /**
    * Determines a subset of message sequence numbers
    * available for retransmission from this stack.
    *
    */
   private Collection getRebroadcastList(Collection rtr)
   {
      ArrayList rebroadcastList = new ArrayList(rtr);
      rebroadcastList.retainAll(receivedMessagesQueue.keySet());
      rtr.removeAll(rebroadcastList);
      Collections.sort(rebroadcastList);
      return rebroadcastList;
   }

   /**
    * TokenTransimitter thread transmits the token to the next member
    * in the logical ring as well as it receives token from the previous
    * member in the ring. Smoothed ring roundtrip time is computed
    * in order to detect token loss.  If the timeout expires AND this
    * stack has received SUSPECT message, recovery protocol is invoked.
    * See recover method for details.
    *
    */
   private class TokenTransmitter extends Thread
   {
      long rtt = 0;
      long timer;
      double srtt = 1000; //1 second to start
      double a = 0.09;
      int timeoutFactor = 10;
      volatile boolean running = false;

      private TokenTransmitter()
      {
         super("TokenTransmitter");
         resetTimeout();
         running = true;
      }

      private void shutDown()
      {
          running = false;
      }

      private void recalculateTimeout()
      {
         long now = System.currentTimeMillis();
         if (timer > 0)
         {
            rtt = now - timer;
            srtt = (1 - a) * srtt + a * rtt;
         }
      }

      private double getTimeout()
      {
         return srtt * timeoutFactor;
      }

      private void resetTimeout()
      {
         timer = System.currentTimeMillis();
      }

      private boolean isRecoveryCompleted(RingToken token)
      {
         if (liveMembersInRecovery.equals(token.getRecoveredMembers()))
         {
            return true;
         }
         return false;
      }

      public void run()
      {
         while (running)
         {
            RingToken token = null;
            int timeout = 0;

             if(node == null) {
                 // sleep some time, then retry
                 Util.sleep(500);
                 continue;
             }

            try
            {
               timeout = (int) getTimeout();

                  if(log.isInfoEnabled()) log.info("timeout(ms)=" + timeout);

               token = (RingToken) node.receiveToken(timeout);

               if (token.getType() == RingToken.OPERATIONAL &&
                       state == RECOVERY_STATE)
               {
                  state = OPERATIONAL_STATE;
               }

               tokenReceived(token);
               recalculateTimeout();

               if (token.getType() == RingToken.RECOVERY &&
                       isRecoveryCompleted(token))
               {

                     if(log.isInfoEnabled()) log.info("all members recovered, injecting operational token");
                  token.setType(RingToken.OPERATIONAL);
               }
               node.passToken(token);
               resetTimeout();
            }
            catch (TokenLostException tle)
            {
               invalidateOnTokenloss();
               state = RECOVERY_STATE;
               recover();
            }
         }
      }
   }
}


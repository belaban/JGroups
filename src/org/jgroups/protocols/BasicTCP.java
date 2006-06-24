package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.util.BoundedList;

import java.util.Vector;
import java.util.Collection;
import java.net.InetAddress;

/**
 * Shared base class for tcpip protocols
 * @author Scott Marlow
 */
public abstract class BasicTCP extends TP {

   /** Should we drop unicast messages to suspected members or not */
   boolean                skip_suspected_members=true;
   /** List the maintains the currently suspected members. This is used so we don't send too many SUSPECT
    * events up the stack (one per message !)
    */
   final BoundedList      suspected_mbrs=new BoundedList(20);
   protected InetAddress	    external_addr=null; // the IP address which is broadcast to other group members
   protected int             start_port=7800;    // find first available port starting at this port
   protected int	            end_port=0;         // maximum port to bind to
   protected long            reaper_interval=0;  // time in msecs between connection reaps
   protected long            conn_expire_time=0; // max time a conn can be idle before being reaped
   /** Use separate send queues for each connection */
   boolean                use_send_queues=true;
   int                    recv_buf_size=150000;
   int                    send_buf_size=150000;
   int                    sock_conn_timeout=2000; // max time in millis for a socket creation in ConnectionTable

   public void sendToAllMembers(byte[] data, int offset, int length) throws Exception {
       Address dest;
       Vector mbrs=(Vector)members.clone();
       for(int i=0; i < mbrs.size(); i++) {
           dest=(Address)mbrs.elementAt(i);
           sendToSingleMember(dest, data, offset, length);
       }
   }

   public void sendToSingleMember(Address dest, byte[] data, int offset, int length) throws Exception {
       if(trace) log.trace("dest=" + dest + " (" + data.length + " bytes)");
       if(skip_suspected_members) {
           if(suspected_mbrs.contains(dest)) {
               if(trace)
                   log.trace("will not send unicast message to " + dest + " as it is currently suspected");
               return;
           }
       }

//        if(dest.equals(local_addr)) {
//            if(!loopback) // if loopback, we discard the message (was already looped back)
//                receive(dest, data, offset, length); // else we loop it back here
//            return;
//        }
       try {
           send(dest, data, offset, length);
       }
       catch(Exception e) {
           if(members.contains(dest)) {
               if(!suspected_mbrs.contains(dest)) {
                   suspected_mbrs.add(dest);
                   passUp(new Event(Event.SUSPECT, dest));
               }
           }
       }
   }

   public String getInfo() {
       StringBuffer sb=new StringBuffer();
       sb.append("connections: ").append(printConnections()).append("\n");
       return sb.toString();
   }

   public void postUnmarshalling(Message msg, Address dest, Address src, boolean multicast) {
       if(multicast)
           msg.setDest(null);
       else
           msg.setDest(dest);
   }

   public void postUnmarshallingList(Message msg, Address dest, boolean multicast) {
       postUnmarshalling(msg, dest, null, multicast);
   }

   public abstract String printConnections();

   public abstract void send(Address dest, byte[] data, int offset, int length) throws Exception;

   public abstract void retainAll(Collection members);

   /** ConnectionTable.Receiver interface */
   public void receive(Address sender, byte[] data, int offset, int length) {
       receive(local_addr, sender, data, offset, length);
   }

   protected void handleDownEvent(Event evt) {
       super.handleDownEvent(evt);
       if(evt.getType() == Event.VIEW_CHANGE) {
           suspected_mbrs.removeAll();
           View v=(View)evt.getArg();
           Vector tmp_mbrs=v != null? v.getMembers() : null;
           if(tmp_mbrs != null) {
               retainAll(tmp_mbrs); // remove all connections from the ConnectionTable which are not members
           }
       }
       else if(evt.getType() == Event.UNSUSPECT) {
           suspected_mbrs.removeElement(evt.getArg());
       }
   }
}

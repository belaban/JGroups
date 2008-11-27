// $Id: DISCARD.java,v 1.26 2008/11/27 15:42:02 vlada Exp $

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * Discards up or down messages based on a percentage; e.g., setting property 'up' to 0.1 causes 10%
 * of all up messages to be discarded. Setting 'down' or 'up' to 0 causes no loss, whereas 1 discards
 * all messages (not very useful).
 */
@Unsupported
public class DISCARD extends Protocol {
    @Property
    double up=0.0;    // probability of dropping up   msgs
    @Property
    double down=0.0;  // probability of dropping down msgs
    @Property
    boolean excludeItself=true;   // if true don't discard messages sent/received in this stack
    Address localAddress;
    int num_down=0, num_up=0;
    
    final Set<Address> ignoredMembers = new HashSet<Address>();
    boolean discard_all=false;

    // number of subsequent unicasts to drop in the down direction
    int drop_down_unicasts=0;

    // number of subsequent multicasts to drop in the down direction
    int drop_down_multicasts=0;

    
    /**
     * All protocol names have to be unique !
     */
    public String getName() {
        return "DISCARD";
    }


    public boolean isDiscardAll() {
        return discard_all;
    }

    public void setDiscardAll(boolean discard_all) {
        this.discard_all=discard_all;
    }

    public boolean isExcludeItself() {
        return excludeItself;
    }
    
    public void setLocalAddress(Address localAddress){
    	this.localAddress =localAddress;
    }

    public void setExcludeItself(boolean excludeItself) {
        this.excludeItself=excludeItself;
    }

    public double getUpDiscardRate() {
        return up;
    }

    public void setUpDiscardRate(double up) {
        this.up=up;
    }

    public double getDownDiscardRate() {
        return down;
    }

    public void setDownDiscardRate(double down) {
        this.down=down;
    }

    public int getDropDownUnicasts() {
        return drop_down_unicasts;
    }

    /**
     * Drop the next N unicasts down the stack
     * @param drop_down_unicasts
     */
    public void setDropDownUnicasts(int drop_down_unicasts) {
        this.drop_down_unicasts=drop_down_unicasts;
    }

    public int getDropDownMulticasts() {
        return drop_down_multicasts;
    }

    public void setDropDownMulticasts(int drop_down_multicasts) {
        this.drop_down_multicasts=drop_down_multicasts;
    }

    /** Messages from this sender will get dropped */
    public void addIgnoreMember(Address sender) {ignoredMembers.add(sender);}

    public void resetIgnoredMembers() {ignoredMembers.clear();}


    public void start() throws Exception {
        super.start();
    }

    public Object up(Event evt) {
        Message msg;
        double r;

        if(evt.getType() == Event.SET_LOCAL_ADDRESS)
            localAddress=(Address)evt.getArg();

        if(evt.getType() == Event.MSG) {
            msg=(Message)evt.getArg();
            Address sender=msg.getSrc();

            if(discard_all && !sender.equals(localAddress)) {                
                return null;
            }

            DiscardHeader dh = (DiscardHeader) msg.getHeader(getName());
			if (dh != null) {
				ignoredMembers.clear();
				ignoredMembers.addAll(dh.dropMessages);
				if (log.isTraceEnabled())
					log.trace("will potentially drop messages from " + ignoredMembers);
			} else {
                boolean dropMessage=ignoredMembers.contains(sender);
                if (dropMessage) {
                    if (log.isTraceEnabled())
                        log.trace("dropping message from " + sender);
                    num_up++;
                    return null;
                }

                if (up > 0) {
					r = Math.random();
					if (r < up) {
						if (excludeItself && sender.equals(localAddress)) {
							if (log.isTraceEnabled())
								log.trace("excluding itself");
						} else {						
							if (log.isTraceEnabled())
								log.trace("dropping message from " + sender);
							num_up++;
							return null;							
						}
					}
				}
			}
        }

        return up_prot.up(evt);
    }


    public Object down(Event evt) {
        Message msg;
        double r;

        if(evt.getType() == Event.MSG) {
            msg=(Message)evt.getArg();
            Address dest=msg.getDest();
            boolean multicast=dest == null || dest.isMulticastAddress();

            if(msg.getSrc() == null)
                msg.setSrc(localAddress);

            if(discard_all) {
                if(dest == null || dest.isMulticastAddress() || dest.equals(localAddress)) {
                    //System.out.println("[" + localAddress + "] down(): looping back " + msg + ", hdrs:\n" + msg.getHeaders());
                    loopback(msg);
                }
                return null;
            }

            if(!multicast && drop_down_unicasts > 0) {
                drop_down_unicasts=Math.max(0, drop_down_unicasts -1);
                return null;
            }

            if(multicast && drop_down_multicasts > 0) {
                drop_down_multicasts=Math.max(0, drop_down_multicasts -1);
                return null;
            }

            if(down > 0) {
                r=Math.random();
                if(r < down) {
                    if(excludeItself && msg.getSrc().equals(localAddress)) {
                        if(log.isTraceEnabled()) log.trace("excluding itself");
                    }
                    else {
                        if(log.isTraceEnabled())
                            log.trace("dropping message");
                        num_down++;
                        return null;
                    }
                }
            }
        }

        return down_prot.down(evt);
    }

    private void loopback(Message msg) {
        final Message rsp=msg.copy(true);
        if(rsp.getSrc() == null)
            rsp.setSrc(localAddress);

        // pretty inefficient: creates one thread per message, okay for testing only
        Thread thread=new Thread(new Runnable() {
            public void run() {
                up_prot.up(new Event(Event.MSG, rsp));
            }
        });
        thread.start();
    }

    public void resetStats() {
        super.resetStats();
        num_down=num_up=0;
    }

    public Map<String,Object> dumpStats() {
        Map<String,Object> m=new HashMap<String,Object>(2);
        m.put("num_dropped_down", new Integer(num_down));
        m.put("num_dropped_up", new Integer(num_up));
        return m;
    }
    
    public static class DiscardHeader extends Header implements Streamable {

		private final Set<Address> dropMessages;
        private static final long serialVersionUID=-2149735838082082084L;

        public DiscardHeader() {
			this.dropMessages= new HashSet<Address>();
		}

		public DiscardHeader(Set<Address> ignoredAddresses) {
			super();
			this.dropMessages= ignoredAddresses;
		}

		public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
			int size = in.readShort();
			if (size > 0) {
				dropMessages.clear();
				for (int i = 0; i < size; i++) {
					dropMessages.add(Util.readAddress(in));
				}
			}
		}

		public void writeTo(DataOutputStream out) throws IOException {
			if (dropMessages != null && !dropMessages.isEmpty()) {
				out.writeShort(dropMessages.size());
				for (Address addr: dropMessages) {
					Util.writeAddress(addr, out);
				}
			} else {
				out.writeShort(0);
			}

		}

		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			Set<Address> tmp = (Set<Address>) in.readObject();
			dropMessages.clear();
			dropMessages.addAll(tmp);
		}

		public void writeExternal(ObjectOutput out) throws IOException {
			out.writeObject(dropMessages);
		}
	}
}

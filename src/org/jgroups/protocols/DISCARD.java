// $Id: DISCARD.java,v 1.16 2007/11/12 11:47:44 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;


/**
 * Discards up or down messages based on a percentage; e.g., setting property 'up' to 0.1 causes 10%
 * of all up messages to be discarded. Setting 'down' or 'up' to 0 causes no loss, whereas 1 discards
 * all messages (not very useful).
 */

public class DISCARD extends Protocol {
    final Vector members=new Vector();
    double up=0.0;    // probability of dropping up   msgs
    double down=0.0;  // probability of dropping down msgs
    boolean excludeItself=true;   // if true don't discard messages sent/received in this stack
    Address localAddress;
    int num_down=0, num_up=0;
    
    final Set<Address> ignoredMembers = new HashSet<Address>();
    boolean discard_all=false;


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

    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("up");
        if(str != null) {
            up=Double.parseDouble(str);
            props.remove("up");
        }

        str=props.getProperty("down");
        if(str != null) {
            down=Double.parseDouble(str);
            props.remove("down");
        }

        str=props.getProperty("excludeitself");
        if(str != null) {
            excludeItself=Boolean.valueOf(str).booleanValue();
            props.remove("excludeitself");
        }

        if(!props.isEmpty()) {
            log.error("DISCARD.setProperties(): these properties are not recognized: " + props);
            return false;
        }
        return true;
    }


    /** Messages from this sender will get dropped */
    public void addIgnoreMember(Address sender) {ignoredMembers.add(sender);}

    public void resetIgnoredMembers() {ignoredMembers.clear();}


    public Object up(Event evt) {
        Message msg;
        double r;

        if(evt.getType() == Event.SET_LOCAL_ADDRESS)
            localAddress=(Address)evt.getArg();

        if(discard_all)
            return null;

        if(evt.getType() == Event.MSG) {
            msg=(Message)evt.getArg();
            Address sender=msg.getSrc();
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
							dropMessage = ignoredMembers.contains(sender);
								
							if (dropMessage) {
								if (log.isTraceEnabled())
									log.trace("dropping message from " + sender);
								num_up++;
								return null;
							}
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

        if(discard_all)
            return null;

        if(evt.getType() == Event.MSG) {
            msg=(Message)evt.getArg();

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

    public void resetStats() {
        super.resetStats();
        num_down=num_up=0;
    }

    public Map dumpStats() {
        Map m=new HashMap(2);
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
			Set tmp = (Set) in.readObject();
			dropMessages.clear();
			dropMessages.addAll(tmp);
		}

		public void writeExternal(ObjectOutput out) throws IOException {
			out.writeObject(dropMessages);
		}
	}
}

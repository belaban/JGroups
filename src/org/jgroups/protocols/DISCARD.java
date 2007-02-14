// $Id: DISCARD.java,v 1.14 2007/02/14 21:52:50 vlada Exp $

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.protocols.pbcast.FLUSH.FlushHeader;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.Vector;


/**
 * Discards up or down messages based on a percentage; e.g., setting property 'up' to 0.1 causes 10%
 * of all up messages to be discarded. Setting 'down' or 'up' to 0 causes no loss, whereas 1 discards
 * all messages (not very useful).
 */

public class DISCARD extends Protocol {
    final Vector members=new Vector();
    double up=0.0;    // probability of dropping up   msgs
    double down=0.0;  // probability of dropping down msgs
    boolean excludeItself=false;   //if true don't discard messages sent/received in this stack
    Address localAddress;
    int num_down=0, num_up=0;
    
    final List<Address> ignoredMembers = new ArrayList<Address>();


    /**
     * All protocol names have to be unique !
     */
    public String getName() {
        return "DISCARD";
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


    public Object up(Event evt) {
        Message msg;
        double r;

        if(evt.getType() == Event.SET_LOCAL_ADDRESS)
            localAddress=(Address)evt.getArg();


        if(evt.getType() == Event.MSG) {
            msg=(Message)evt.getArg();
            DiscardHeader dh = (DiscardHeader) msg.getHeader(getName());
			if (dh != null) {
				ignoredMembers.clear();
				ignoredMembers.addAll(dh.dropMessagesAddressList);
				if (log.isTraceEnabled())
					log.trace("will potentially drop messages from " + ignoredMembers);
			} else {
				if (up > 0) {
					r = Math.random();
					if (r < up) {
						if (excludeItself && msg.getSrc().equals(localAddress)) {
							if (log.isTraceEnabled())
								log.trace("excluding itself");
						} else {
							boolean dropMessage = ignoredMembers.isEmpty() || 
												(!ignoredMembers.isEmpty() && ignoredMembers.contains(msg.getSrc()));
								
							if (dropMessage) {
								if (log.isTraceEnabled())
									log.trace("dropping message from " + msg.getSrc());
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

		private final List<Address> dropMessagesAddressList;

		public DiscardHeader() {
			this.dropMessagesAddressList = new ArrayList<Address>();			
		}

		public DiscardHeader(List<Address> ignoredAddresses) {
			super();
			this.dropMessagesAddressList = ignoredAddresses;
		}

		public void readFrom(DataInputStream in) throws IOException,
				IllegalAccessException, InstantiationException {
			int size = in.readShort();
			if (size > 0) {
				dropMessagesAddressList.clear();
				for (int i = 0; i < size; i++) {
					dropMessagesAddressList.add(Util.readAddress(in));
				}
			}
		}

		public void writeTo(DataOutputStream out) throws IOException {
			if (dropMessagesAddressList != null && !dropMessagesAddressList.isEmpty()) {
				out.writeShort(dropMessagesAddressList.size());
				for (Iterator iter = dropMessagesAddressList.iterator(); iter
						.hasNext();) {
					Address address = (Address) iter.next();
					Util.writeAddress(address, out);
				}
			} else {
				out.writeShort(0);
			}

		}

		@SuppressWarnings("unchecked")
		public void readExternal(ObjectInput in) throws IOException,
				ClassNotFoundException {
			List tmp = (List) in.readObject();
			dropMessagesAddressList.clear();
			dropMessagesAddressList.addAll(tmp);

		}

		public void writeExternal(ObjectOutput out) throws IOException {
			out.writeObject(dropMessagesAddressList);
		}
	}
}

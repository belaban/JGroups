
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.Event;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.util.*;
import java.util.List;


/**
 * Discards up or down messages based on a percentage; e.g., setting property 'up' to 0.1 causes 10%
 * of all up messages to be discarded. Setting 'down' or 'up' to 0 causes no loss, whereas 1 discards
 * all messages (not very useful).
 */
@Unsupported
@MBean(description="Discards messages")
public class DISCARD extends Protocol {
    @Property
    double up=0.0;    // probability of dropping up   msgs

    @Property
    double down=0.0;  // probability of dropping down msgs

    @Property
    boolean excludeItself=true;   // if true don't discard messages sent/received in this stack
    Address localAddress;

    @ManagedAttribute(description="Number of dropped down messages",name="droppedDownMessages")
    int num_down=0;

    @ManagedAttribute(description="Number of dropped up messages",name="droppedUpMessages")
    int num_up=0;
    
    final Set<Address> ignoredMembers = new HashSet<Address>();


    final Collection<Address> members=new ArrayList<Address>();

    @Property(description="drop all messages (up or down)",writable=true)
    boolean discard_all=false;

    @Property(description="Number of subsequent unicasts to drop in the down direction",writable=true)
    int drop_down_unicasts=0;

    @Property(description="Number of subsequent multicasts to drop in the down direction",writable=true)
    int drop_down_multicasts=0;

    private DiscardDialog discard_dialog=null;

    @Property(name="gui", description="use a GUI or not")
    protected boolean use_gui=false;

    public boolean isDiscardAll() {
        return discard_all;
    }

    public DISCARD setDiscardAll(boolean discard_all) {
        this.discard_all=discard_all; return this;
    }

    public boolean isExcludeItself() {
        return excludeItself;
    }
    
    public void setLocalAddress(Address localAddress){
    	this.localAddress =localAddress;
        if(discard_dialog != null)
            discard_dialog.setTitle(localAddress != null? localAddress.toString() : "n/a");
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

    public void removeIgnoredMember(Address member) {ignoredMembers.remove(member);}

    public void resetIgnoredMembers() {ignoredMembers.clear();}


    @ManagedOperation
    public void startGui() {
        if(discard_dialog == null) {
            discard_dialog=new DiscardDialog();
            discard_dialog.init();
            discard_dialog.setTitle(localAddress != null? localAddress.toString() : "n/a");
            discard_dialog.handleView(members);
        }
    }

    @ManagedOperation
    public void stopGui() {
        if(discard_dialog != null)
            discard_dialog.dispose();
        discard_dialog=null;
    }

    public void start() throws Exception {
        super.start();
        if(use_gui) {
            discard_dialog=new DiscardDialog();
            discard_dialog.init();
        }
    }

    public void stop() {
        super.stop();
        if(discard_dialog != null)
            discard_dialog.dispose();
    }

    public Object up(Event evt) {
        Message msg;
        double r;

        if(evt.getType() == Event.SET_LOCAL_ADDRESS) {
            localAddress=(Address)evt.getArg();
            if(discard_dialog != null)
                discard_dialog.setTitle("Discard dialog (" + localAddress + ")");
        }

        if(evt.getType() == Event.MSG) {
            msg=(Message)evt.getArg();
            Address sender=msg.getSrc();

            if(discard_all && !sender.equals(localAddress)) {                
                return null;
            }

            DiscardHeader dh = (DiscardHeader) msg.getHeader(this.id);
			if (dh != null) {
				ignoredMembers.clear();
				ignoredMembers.addAll(dh.dropMessages);
				if (log.isTraceEnabled())
					log.trace("will potentially drop messages from " + ignoredMembers);
			} else {
                boolean dropMessage=ignoredMembers.contains(sender);
                if (dropMessage) {
                    if (log.isTraceEnabled())
                        log.trace(localAddress + ": dropping message from " + sender);
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
								log.trace(localAddress + ": dropping message from " + sender);
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

        switch(evt.getType()) {
            case Event.MSG:
            msg=(Message)evt.getArg();
            Address dest=msg.getDest();
            boolean multicast=dest == null;

            if(msg.getSrc() == null)
                msg.setSrc(localAddress);

            if(discard_all) {
                if(dest == null || dest.equals(localAddress)) {
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
                break;
            case Event.VIEW_CHANGE:
                View view=(View)evt.getArg();
                List<Address> mbrs=view.getMembers();
                members.clear();
                members.addAll(mbrs);
                ignoredMembers.retainAll(mbrs); // remove all non members
                if(discard_dialog != null)
                    discard_dialog.handleView(mbrs);
                break;

            case Event.SET_LOCAL_ADDRESS:
                localAddress=(Address)evt.getArg();
                if(discard_dialog != null)
                    discard_dialog.setTitle("Discard dialog (" + localAddress + ")");
                break;
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


    
    public static class DiscardHeader extends Header {
		private final Set<Address> dropMessages;

        public DiscardHeader() {
			this.dropMessages= new HashSet<Address>();
		}

		public DiscardHeader(Set<Address> ignoredAddresses) {
			super();
			this.dropMessages= ignoredAddresses;
		}

		public void readFrom(DataInput in) throws Exception {
			int size = in.readShort();
			if (size > 0) {
				dropMessages.clear();
				for (int i = 0; i < size; i++) {
					dropMessages.add(Util.readAddress(in));
				}
			}
		}

		public void writeTo(DataOutput out) throws Exception {
			if (dropMessages != null && !dropMessages.isEmpty()) {
				out.writeShort(dropMessages.size());
				for (Address addr: dropMessages) {
					Util.writeAddress(addr, out);
				}
			} else {
				out.writeShort(0);
			}

		}

        public int size() {
            return (int)Util.size(dropMessages);
        }
    }


    private class DiscardDialog extends JFrame implements ActionListener {
        private JButton start_discarding_button=new JButton("start discarding");
        private JButton stop_discarding_button=new JButton("stop discarding");
        JPanel checkboxes=new JPanel();


        private DiscardDialog() {
        }

        void init() {
            getContentPane().setLayout(new BoxLayout(getContentPane(), BoxLayout.Y_AXIS));
            checkboxes.setLayout(new BoxLayout(checkboxes, BoxLayout.Y_AXIS));
            getContentPane().add(start_discarding_button);
            getContentPane().add(stop_discarding_button);
            start_discarding_button.addActionListener(this);
            stop_discarding_button.addActionListener(this);
            getContentPane().add(checkboxes);
            pack();
            setVisible(true);
            setTitle(localAddress != null? localAddress.toString() : "n/a");
        }


        public void actionPerformed(ActionEvent e) {
            String command=e.getActionCommand();
            if(command.startsWith("start")) {
                discard_all=true;
            }
            else if(command.startsWith("stop")) {
                discard_all=false;
                Component[] comps=checkboxes.getComponents();
                for(Component c: comps) {
                    if(c instanceof JCheckBox) {
                        ((JCheckBox)c).setSelected(false);
                    }
                }
            }
        }

        void handleView(Collection<Address> mbrs) {
            checkboxes.removeAll();
            for(final Address addr: mbrs) {
                final MyCheckBox box=new MyCheckBox("discard traffic from " + addr, addr);
                box.addActionListener(new ActionListener() {
                    public void actionPerformed(ActionEvent e) {
                        if(box.isSelected()) {
                            ignoredMembers.add(addr);
                        }
                        else {
                            ignoredMembers.remove(addr);
                        }
                    }
                });
                checkboxes.add(box);
            }

            for(Component comp: checkboxes.getComponents()) {
                MyCheckBox box=(MyCheckBox)comp;
                if(ignoredMembers.contains(box.mbr))
                    box.setSelected(true);
            }
            pack();
        }
    }

    private static class MyCheckBox extends JCheckBox {
        final Address mbr;

        public MyCheckBox(String name, Address member) {
            super(name);
            this.mbr=member;
        }

        public String toString() {
            return super.toString() + " [mbr=" + mbr + "]";
        }
    }
}
package org.jgroups.demos;

// prepend 'bridge.' to the next 2 imports if
// you want to use this class w/ JGroups-ME
import java.util.Iterator;
import java.util.LinkedList;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.View;
import org.jgroups.blocks.PullPushAdapter;
import org.jgroups.util.Util;

public abstract class ChatCore implements MessageListener, MembershipListener {
	Channel channel;

	PullPushAdapter ad;

	Thread mainThread;

	static final String group_name = "ChatGroup";

	String props = null;

	String username = null;

	LinkedList history = new LinkedList();

	public ChatCore(String props) {
		this.props = props;
		try {
			username = System.getProperty("user.name");
		} catch (Throwable t) {
		}
	}

	abstract void post(String msg);

	public void link() {

		try {
			channel = new JChannel(props);
			System.out.println("Connecting to " + group_name);
			channel.connect(group_name);
			ad = new PullPushAdapter(channel, this, this);
			channel.getState(null, 5000);
		} catch (Exception e) {
			e.printStackTrace();
			post(e.toString());
		}

	}

	public void dumpHist() {

		if (!history.isEmpty()) {
			for (Iterator it = history.iterator(); it.hasNext();) {
				String s = (String) it.next();
				post(s + "\n");
				System.err.print(s + "\n");
			}
		}
	}

	/* -------------------- Interface MessageListener ------------------- */

	public void receive(Message msg) {
		String o;

		try {
			o = new String(msg.getBuffer());
			System.err.print(o + " [" + msg.getSrc() + "]\n");
			post(o + " [" + msg.getSrc() + "]\n");
			history.add(o);
		} catch (Exception e) {
			post("Chat.receive(): " + e);
			System.err.print("Chat.receive(): " + e);
		}
	}

	public byte[] getState() {
		try {
			return Util.objectToByteBuffer(history);
		} catch (Exception e) {
			return null;
		}
	}

	public void setState(byte[] state) {
		try {
			history = (LinkedList) Util.objectFromByteBuffer(state);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/* ----------------- End of Interface MessageListener --------------- */

	/* ------------------- Interface MembershipListener ----------------- */

	public void viewAccepted(View new_view) {
		post("Received view " + new_view + '\n');
		System.err.print("Received view " + new_view + '\n');
	}

	public void suspect(Address suspected_mbr) {
	}

	public void block() {
	}

	public void stop() {
		System.out.print("Stopping PullPushAdapter");
		ad.stop();
		System.out.println(" -- done");
	}

	public void disconnect() {
		System.out.print("Disconnecting the channel");
		channel.disconnect();
		System.out.println(" -- done");
	}

	public void close() {
		System.out.print("Closing the channel");
		channel.close();
		System.out.println(" -- done");
	}

	/* --------------- End of Interface MembershipListener -------------- */

	protected synchronized void handleLeave() {
		try {
			stop();
			disconnect();
			close();
		} catch (Exception e) {
			e.printStackTrace();
			System.err
					.print("Failed leaving the group: " + e.toString() + '\n');
			post("Failed leaving the group: " + e.toString() + '\n');
		}
	}

	protected void handleSend(String txt) {
		String tmp = username + ": " + txt;
		try {
			// for the sake of jgroups-me compatibility we stick with
			// byte buffers and Streamable *only* (not Serializable)
			Message msg = new Message(null, null, tmp.getBytes());
			channel.send(msg);
		} catch (Exception e) {
			System.err.print("Failed sending message: " + e.toString() + '\n');
			post("Failed sending message: " + e.toString() + '\n');
		}
	}

}

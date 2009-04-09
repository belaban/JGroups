// $Id: GroupRequestPull.java,v 1.12 2009/04/09 09:11:20 belaban Exp $

package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.PullPushAdapter;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Vector;




/**
 *
 * @author Bela Ban
 */
public class GroupRequestPull implements MessageListener, MembershipListener, Transport {
    PullPushAdapter      adapter=null;
    Channel              ch=null;
    String               props=null;
    GroupRequest         group_req=null;
    static final String  HDRNAME="GroupRequestPullHeader";
    Vector               members=new Vector();



    

    GroupRequestPull(String props) {
        this.props=props;
    }
    

    void start() throws Throwable {
        ch=new JChannel(props);
        ch.connect("GroupRequestPull-Group");
        adapter=new PullPushAdapter(ch, this, this);
        loop();
        adapter.stop();
        ch.close();
    }
    

    void loop() throws Throwable {
        boolean looping=true;
        int     c;

        while(looping) {
            System.out.println("Members are " + ch.getView().getMembers() + "\n<enter to send a new group request>");
            System.out.flush();
            c=System.in.read();
            if(c == 'q')
                looping=false;
            System.in.skip(System.in.available());
            sendGroupRequest();
        }
    }


    void sendGroupRequest() throws Throwable {
        Message msg=new Message();
        RspList lst;
	
        msg.putHeader(HDRNAME, new MyHeader(MyHeader.REQUEST));
        group_req=new GroupRequest(msg,
                                   this, // as Transport
                                   members, // all current members
                                   GroupRequest.GET_ALL);

        group_req.execute();
        lst=group_req.getResults();
        System.out.println("-- received " + lst.size() + " results:");
        for(int i=0; i < lst.size(); i++) {
            System.out.println(lst.elementAt(i));
        }
        System.out.println();
    }
    
    /* --------------------------- Interface MessageListener -------------------------- */

    public void receive(Message msg) {
        MyHeader hdr=(MyHeader)msg.removeHeader(HDRNAME);
        Message  rsp;

        if(hdr == null) {
            System.err.println("GroupRequestPull.receive(): header for " + HDRNAME + " was null");
            return;
        }
        if(hdr.type == MyHeader.RESPONSE) {
            if(group_req != null) {
                Address sender=msg.getSrc();
                Object retval=null;
                try {
                    retval=Util.objectFromByteBuffer(msg.getBuffer());
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
                group_req.receiveResponse(retval, sender);
            }
        }
        else if(hdr.type == MyHeader.REQUEST) {
            // System.out.println("-- received REQUEST from " + msg.getSrc());
            rsp=new Message(msg.getSrc());
            rsp.putHeader(HDRNAME, new MyHeader(MyHeader.RESPONSE));
            rsp.setObject("Hello from member " + ch.getAddress());
            try {
                adapter.send(rsp);
            }
            catch(Exception ex) {
                System.err.println("GroupRequestPull.receive(): failure sending response: " + ex);
            }
        }
        else {
            System.err.println("GroupRequestPull.receive(): header type of " + hdr.type + " not known");
        }
    }

    public byte[] getState() {
        return null;
    }

    public void setState(byte[] state) {
        ;
    }

    /* ----------------------- End of Interface MessageListener ------------------------ */




    /* --------------------------- Interface MembershipListener -------------------------- */

    public void viewAccepted(View new_view) {
        System.out.println("** viewAccepted(): " + new_view);
        if(new_view != null && new_view.getMembers().size() > 0) {
            members.removeAllElements();
            members.addAll(new_view.getMembers());
        }
        if(group_req != null)
            group_req.viewChange(new_view);
    }

    public void suspect(Address suspected_mbr) {
        System.out.println("** suspect(): " + suspected_mbr);
        if(group_req != null)
            group_req.suspect(suspected_mbr);
    }

    public void block() {
	
    }

    /* ----------------------- End of Interface MembershipListener ----------------------- */


    /* --------------------------- Interface Transport ------------------------------------ */
    /** Used by GroupRequest to send messages */ 
    public void send(Message msg) throws Exception {
        if(adapter == null) {
            System.err.println("GroupRequestPull.send(): adapter is null, cannot send message");
        }
        else
            adapter.send(msg);
    }

    public Object receive(long timeout) throws Exception {
        return null;
    }
    /* ------------------------ End of Interface Transport -------------------------------- */


    public static void main(String[] args) {
        String props=null;

        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            help();
            return;
        }




        try {
            new GroupRequestPull(props).start();
        }
        catch(Throwable ex) {
            ex.printStackTrace();
        }
    }


    static void help() {
        System.out.println("GroupRequestPull [-help] [-props <properties>]");
    }




    public static class MyHeader extends Header {
        public static final int REQUEST  = 1;
        public static final int RESPONSE = 2;

        int type=0;


        public MyHeader() {
	    
        }

        public MyHeader(int type) {
            this.type=type;
        }
	
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(type);
        }
	
	
	
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readInt();
        }
    }

}




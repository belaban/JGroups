package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * Simple discovery protocol which uses a file on shared storage such as an SMB share, NFS mount or S3. The local
 * address information, e.g. UUID and physical addresses mappings are written to the file and the content is read and
 * added to our transport's UUID-PhysicalAddress cache.
 * @author Bela Ban
 * @version $Id: FILE_PING.java,v 1.2 2009/04/23 15:01:46 belaban Exp $
 */
public class FILE_PING extends Discovery {
    private static final String name="FILE_PING";

    /* -----------------------------------------    Properties     -------------------------------------------------- */


    @Property(description="The absolute path of the shared file")
    @ManagedAttribute(description="location of the shared file used for discovery")
    private String location=null;


    /* --------------------------------------------- Fields ------------------------------------------------------ */

    protected File file=null;




    public String getName() {
        return name;
    }

    public void init() throws Exception {
        super.init();
        file=new File(location);
        boolean created=file.createNewFile();
        if(created &&  log.isDebugEnabled())
            log.debug("file " + file.getPath() + " created");
        if(!file.exists())
            throw new FileNotFoundException(file.getPath());
        if(!file.canRead() || !file.canWrite())
            throw new IllegalStateException("file " + file.getPath() + " is not writable");
        if(!created && log.isDebugEnabled())
            log.debug("file " + file.getPath() + " found");
    }



    public void stop() {
        super.stop();
    }

    public void destroy(){
        super.destroy();
    }



    public void sendGetMembersRequest(String cluster_name) throws Exception{
        PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
        List<PhysicalAddress> physical_addrs=Arrays.asList(physical_addr);
        PingData data=new PingData(local_addr, null, false, UUID.get(local_addr), physical_addrs);
        List<PingData> other_mbrs=readFromFile();

        if(!contains(local_addr, other_mbrs)) {
            appendToFile(data);
            other_mbrs.add(data);
        }

        boolean removed=false;
        synchronized(members) {
            if(!members.isEmpty()) {
                for(Iterator<PingData> it=other_mbrs.iterator(); it.hasNext();) {
                    PingData next=it.next();
                    if(!members.contains(next.getAddress())) {
                        it.remove();
                        removed=true;
                    }
                }
            }
        }
        if(removed) {
            writeAllToFile(other_mbrs);
        }

        // 1. Send GET_MBRS_REQ message to members listed in the file
        for(PingData tmp: other_mbrs) {
            List<PhysicalAddress> dests=tmp.getPhysicalAddrs();
            if(dests == null)
                continue;
            for(final PhysicalAddress dest: dests) {
                if(dest.equals(physical_addr))
                    continue;
                PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ, data, cluster_name);
                final Message msg=new Message(dest);
                msg.setFlag(Message.OOB);
                msg.putHeader(getName(), hdr); // needs to be getName(), so we might get "MPING" !
                // down_prot.down(new Event(Event.MSG,  msg));
                if(log.isTraceEnabled())
                    log.trace("[FIND_INITIAL_MBRS] sending PING request to " + msg.getDest());
                timer.submit(new Runnable() {
                    public void run() {
                        try {
                            down_prot.down(new Event(Event.MSG, msg));
                        }
                        catch(Exception ex){
                            if(log.isErrorEnabled())
                                log.error("failed sending discovery request to " + dest, ex);
                        }
                    }
                });
            }
        }
    }

    private static boolean contains(Address addr, List<PingData> list) {
        if(addr == null || list == null)
            return false;
        for(PingData data: list) {
            if(addr.equals(data.getAddress()))
                return true;
        }
        return false;
    }

    private List<PingData> readFromFile() {
        List<PingData> retval=new ArrayList<PingData>();
        DataInputStream in=null;

        try {
            in=new DataInputStream(new FileInputStream(file));
            for(;;) {
                PingData tmp=new PingData();
                tmp.readFrom(in);
                retval.add(tmp);
            }
        }
        catch(Exception e) {
        }
        finally {
            Util.close(in);
        }
        return retval;
    }

    private void appendToFile(PingData data) {
        DataOutputStream out=null;
        try {
            out=new DataOutputStream(new FileOutputStream(file, true)); // append at the end
            data.writeTo(out);
        }
        catch(Exception e) {
        }
        finally {
            Util.close(out);
        }
    }

    private void writeAllToFile(List<PingData> list) {
        DataOutputStream out=null;
        try {
            out=new DataOutputStream(new FileOutputStream(file, false)); // overwrite
            for(PingData data: list)
                data.writeTo(out);
        }
        catch(Exception e) {
        }
        finally {
            Util.close(out);
        }
    }


   /* private void addResponses(List<PingData> rsps) {
        // add physical address (if available) to transport's cache
        for(PingData rsp: rsps) {
            Address logical_addr=rsp.getAddress();
            List<PhysicalAddress> physical_addrs=rsp.getPhysicalAddrs();
            PhysicalAddress physical_addr=physical_addrs != null && !physical_addrs.isEmpty()?
                    physical_addrs.get(0) : null;
            if(logical_addr != null && physical_addr != null)
                down(new Event(Event.SET_PHYSICAL_ADDRESS, new Tuple<Address,PhysicalAddress>(logical_addr, physical_addr)));
            if(logical_addr != null && rsp.getLogicalName() != null)
                UUID.add((UUID)logical_addr, rsp.getLogicalName());

            synchronized(ping_responses) {
                for(Responses tmp: ping_responses)
                    tmp.addResponse(rsp);
            }
        }
    }*/


}
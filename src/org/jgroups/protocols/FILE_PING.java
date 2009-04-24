package org.jgroups.protocols;

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
import java.util.List;


/**
 * Simple discovery protocol which uses a file on shared storage such as an SMB share, NFS mount or S3. The local
 * address information, e.g. UUID and physical addresses mappings are written to the file and the content is read and
 * added to our transport's UUID-PhysicalAddress cache.<p/>
 * The design is at doc/design/FILE_PING.txt
 * @author Bela Ban
 * @version $Id: FILE_PING.java,v 1.3 2009/04/24 08:31:21 belaban Exp $
 */
public class FILE_PING extends Discovery {
    private static final String name="FILE_PING";
    private static final String SUFFIX=".node";

    /* -----------------------------------------    Properties     -------------------------------------------------- */


    @Property(description="The absolute path of the shared file")
    @ManagedAttribute(description="location of the shared file used for discovery")
    private String location=File.separator + "tmp" + File.separator + "jgroups";


    /* --------------------------------------------- Fields ------------------------------------------------------ */
    protected File root_dir=null;
    protected FilenameFilter filter;


    public String getName() {
        return name;
    }

    public void init() throws Exception {
        super.init();
        root_dir=new File(location);
        if(root_dir.exists()) {
            if(!root_dir.isDirectory())
                throw new IllegalArgumentException("location " + root_dir.getPath() + " is not a directory");
        }
        else {
            root_dir.mkdirs();
        }
        if(!root_dir.exists())
            throw new IllegalArgumentException("location " + root_dir.getPath() + " could not be accessed");

        filter=new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(SUFFIX);
            }
        };
    }



    public void stop() {
        super.stop();
    }

    public void destroy(){
        super.destroy();
    }



    public void sendGetMembersRequest(String cluster_name) throws Exception{
        List<PingData> other_mbrs=readAll(cluster_name);
        PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
        List<PhysicalAddress> physical_addrs=Arrays.asList(physical_addr);
        PingData data=new PingData(local_addr, null, false, UUID.get(local_addr), physical_addrs);

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

        // Write my own data to file
        writeToFile(data, cluster_name);

    }



    /**
     * Reads all information from the given directory under clustername
     * @return
     */
   private List<PingData> readAll(String clustername) {
        List<PingData> retval=new ArrayList<PingData>();
        File dir=new File(root_dir, clustername);
        if(!dir.exists())
            dir.mkdir();

        File[] files=dir.listFiles(filter);
        if(files != null) {
            for(File file: files)
                retval.add(readFile(file));
        }
        return retval;
    }

    private static PingData readFile(File file) {
        PingData retval=null;
        DataInputStream in=null;

        try {
            in=new DataInputStream(new FileInputStream(file));
            PingData tmp=new PingData();
            tmp.readFrom(in);
            return tmp;
        }
        catch(Exception e) {
        }
        finally {
            Util.close(in);
        }
        return retval;
    }

    private void writeToFile(PingData data, String clustername) {
        DataOutputStream out=null;
        File dir=new File(root_dir, clustername);
        if(!dir.exists())
            dir.mkdir();

        File file=new File(dir, data.getAddress().toString() + SUFFIX);

        try {
            out=new DataOutputStream(new FileOutputStream(file));
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
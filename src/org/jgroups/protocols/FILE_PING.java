package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Experimental;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.jgroups.util.Promise;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Collection;


/**
 * Simple discovery protocol which uses a file on shared storage such as an SMB share, NFS mount or S3. The local
 * address information, e.g. UUID and physical addresses mappings are written to the file and the content is read and
 * added to our transport's UUID-PhysicalAddress cache.<p/>
 * The design is at doc/design/FILE_PING.txt
 * @author Bela Ban
 * @version $Id: FILE_PING.java,v 1.12 2009/12/11 13:02:28 belaban Exp $
 */
@Experimental
public class FILE_PING extends Discovery {
    protected static final String SUFFIX=".node";

    /* -----------------------------------------    Properties     -------------------------------------------------- */


    @Property(description="The absolute path of the shared file")
    protected String location=File.separator + "tmp" + File.separator + "jgroups";


    /* --------------------------------------------- Fields ------------------------------------------------------ */
    protected File root_dir=null;
    protected FilenameFilter filter;


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



    public void sendGetMembersRequest(String cluster_name, Promise promise, boolean return_views_only) throws Exception{
        List<PingData> existing_mbrs=readAll(cluster_name);
        PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
        List<PhysicalAddress> physical_addrs=Arrays.asList(physical_addr);
        PingData data=new PingData(local_addr, null, false, UUID.get(local_addr), physical_addrs);

        // If we don't find any files, return immediately
        if(existing_mbrs.isEmpty()) {
            if(promise != null) {
                promise.setResult(null);
            }
        }
        else {

            // 1. Send GET_MBRS_REQ message to members listed in the file
            for(PingData tmp: existing_mbrs) {
                Collection<PhysicalAddress> dests=tmp.getPhysicalAddrs();
                if(dests == null)
                    continue;
                for(final PhysicalAddress dest: dests) {
                    if(dest.equals(physical_addr))
                        continue;
                    PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ, data, cluster_name);
                    hdr.return_view_only=return_views_only;
                    final Message msg=new Message(dest);
                    msg.setFlag(Message.OOB);
                    msg.putHeader(getName(), hdr); // needs to be getName(), so we might get "MPING" !
                    // down_prot.down(new Event(Event.MSG,  msg));
                    if(log.isTraceEnabled())
                        log.trace("[FIND_INITIAL_MBRS] sending PING request to " + msg.getDest());
                    timer.execute(new Runnable() {
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

        // Write my own data to file
        writeToFile(data, cluster_name);
    }



    /**
     * Reads all information from the given directory under clustername
     * @return
     */
   protected List<PingData> readAll(String clustername) {
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

    protected static PingData readFile(File file) {
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

    protected void writeToFile(PingData data, String clustername) {
        DataOutputStream out=null;
        File dir=new File(root_dir, clustername);
        if(!dir.exists())
            dir.mkdir();

        File file=new File(dir, local_addr.toString() + SUFFIX);
        file.deleteOnExit();

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



}
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.util.Util;
import org.jgroups.util.Promise;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * Simple discovery protocol which uses a file on shared storage such as an SMB share, NFS mount or S3. The local
 * address information, e.g. UUID and physical addresses mappings are written to the file and the content is read and
 * added to our transport's UUID-PhysicalAddress cache.<p/>
 * The design is at doc/design/FILE_PING.txt
 * @author Bela Ban
 * @version $Id: FILE_PING.java,v 1.5.2.3 2009/04/27 08:37:23 belaban Exp $
 */
public class FILE_PING extends Discovery {
    private static final String name="FILE_PING";
    private static final String SUFFIX=".node";

    /* -----------------------------------------    Properties     -------------------------------------------------- */


    // location of the shared directory used for discovery"
    private String location=File.separator + "tmp" + File.separator + "jgroups";


    /* --------------------------------------------- Fields ------------------------------------------------------ */
    protected File root_dir=null;
    protected FilenameFilter filter;


    public String getName() {
        return name;
    }


    public boolean setProperties(Properties props) {
           String str;

           str=props.getProperty("location");
           if(str != null) {
               location=str;
               props.remove("location");
           }

           return super.setProperties(props);
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


    public void sendGetMembersRequest(Promise promise) throws Exception{
        String cluster_name=group_addr;
        List<Address> existing_mbrs=readAll(cluster_name);

        // If we don't find any files, return immediately
        if(existing_mbrs.isEmpty() || (existing_mbrs.size() == 1 && existing_mbrs.contains(local_addr))) {
            if(promise != null) {
                promise.setResult(true);
            }
        }
        else {

            // 1. Send GET_MBRS_REQ message to members listed in the file
            for(final Address dest: existing_mbrs) {
                if(dest.equals(local_addr))
                    continue;
                PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ, null);
                final Message msg=new Message(dest);
                msg.setFlag(Message.OOB);
                msg.putHeader(getName(), hdr); // needs to be getName(), so we might get "MPING" !
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
        writeToFile(local_addr, cluster_name);
    }



    /**
     * Reads all information from the given directory under clustername
     * @return
     */
   private List<Address> readAll(String clustername) {
        List<Address> retval=new ArrayList<Address>();
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

    private static Address readFile(File file) {
        Address retval=null;
        DataInputStream in=null;

        try {
            in=new DataInputStream(new FileInputStream(file));
            return Util.readAddress(in);
        }
        catch(Exception e) {
        }
        finally {
            Util.close(in);
        }
        return retval;
    }

    private void writeToFile(Address addr, String clustername) {
        DataOutputStream out=null;
        File dir=new File(root_dir, clustername);
        if(!dir.exists())
            dir.mkdir();

        File file=new File(dir, addr.toString() + SUFFIX);
        file.deleteOnExit();

        try {
            out=new DataOutputStream(new FileOutputStream(file));
            Util.writeAddress(addr, out);
        }
        catch(Exception e) {
        }
        finally {
            Util.close(out);
        }
    }



}
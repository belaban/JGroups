package org.jgroups.protocols;

import org.jgroups.annotations.Property;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.PhysicalAddress;
import org.jgroups.Event;
import org.jgroups.Address;
import org.jgroups.util.UUID;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;

import java.io.*;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;


/**
 * Simple discovery protocol which uses a file on shared storage such as an SMB share, NFS mount or S3. The local
 * address information, e.g. UUID and physical addresses mappings are written to the file and the content is read and
 * added to our transport's UUID-PhysicalAddress cache.
 * @author Bela Ban
 * @version $Id: FILE_PING.java,v 1.1 2009/04/23 12:38:31 belaban Exp $
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
        if(!other_mbrs.contains(data))
            writeToFile(data);
        addResponses(other_mbrs);
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

    private void writeToFile(PingData data) {
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


    private void addResponses(List<PingData> rsps) {
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
    }


}
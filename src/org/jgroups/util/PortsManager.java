package org.jgroups.util;

import java.io.*;
import java.util.*;

/**
 * Maintains a list of ports used on this host, associated with time stamps. The ports are persistet into the
 * temp file system.
 * @author Bela Ban
 * @version $Id: PortsManager.java,v 1.1 2007/11/27 10:22:53 belaban Exp $
 */
public class PortsManager {
    private String filename="jgroups-ports.txt";
    private String temp_dir=System.getProperty("java.io.tmpdir", "/tmp");
    private final static String file_separator=System.getProperty("file.separator", "/");
    private final String absolute_name=temp_dir + file_separator + filename;

    /** Time after which a port can be removed and re-allocated */
    private long expiry_time=60 * 1000L;

    public PortsManager(long expiry_time) {
        this.expiry_time=expiry_time;
    }

    public PortsManager(long expiry_time, String filename, String temp_dir) {
        this.expiry_time=expiry_time;
        this.filename=filename;
        this.temp_dir=temp_dir;
    }

    /** Loads the file, weeds out expired ports, returns the next available port and saves the new port in the file */
    public int getNextAvailablePort(int start_port) {
        Map<Integer,Long> map;
        int retval=-1;

        try {
            map=load();
            long current_time=System.currentTimeMillis(), timestamp;
            if(map.isEmpty()) {
                map.put(start_port, current_time);
                store(map);
                return start_port;
            }

            // wed out expired ports
            Map.Entry<Integer,Long> entry;
            for(Iterator<Map.Entry<Integer,Long>> it=map.entrySet().iterator(); it.hasNext();) {
                entry=it.next();
                timestamp=entry.getValue();
                if(current_time - timestamp >= expiry_time) {
                    it.remove();
                }
            }

            // find next available port
            Set<Integer> ports=map.keySet();
            while(ports.contains(start_port)) {
                start_port++;
            }
            map.put(start_port, System.currentTimeMillis());
            store(map);
            return start_port;
        }
        catch(IOException e) {
            return retval;
        }
    }

    /** Loads the file, removes the port (if existent) and closes the file again */
    public void removePort(int port) {
        Map<Integer,Long> map;

        try {
            map=load();
            if(map.isEmpty()) {
                return;
            }
            map.remove(port);
            store(map);
        }
        catch(IOException e) {
        }
    }


    private Map<Integer,Long> load() throws IOException {
        InputStream in=null;
        Map<Integer,Long> retval=new HashMap<Integer,Long>();
        try {
            in=new FileInputStream(absolute_name);
            Properties props=new Properties();
            props.load(in);
            for(Iterator<Map.Entry<Object,Object>> it=props.entrySet().iterator(); it.hasNext();) {
                Map.Entry<Object,Object> entry=it.next();
                Object key=entry.getKey(), val=entry.getValue();
                retval.put(Integer.parseInt((String)val), Long.parseLong((String)key));
            }
            return retval;
        }
        catch(Throwable t) {
            return retval;
        }
        finally {
            Util.close(in);
        }
    }

    private void store(Map<Integer,Long> map) throws IOException {
        OutputStream out=null;
        try {
            out=new FileOutputStream(absolute_name);
            Properties props=new Properties();
            props.putAll(map);
            props.store(out, "persistent JGroups ports");
        }
        finally {
            Util.close(out);
        }
    }
}

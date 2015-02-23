package org.jgroups.util;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.conf.ClassConfigurator;

import java.util.HashMap;
import java.util.Map;

/**
 * Open addressing based implementation of a hashmap (not supporting the Map interface though) for message
 * headers. The keys are shorts (IDs) and the values Headers, and they're stored in 2 arrays: an ID array and a headers
 * array. The indices of the IDs array corespond with the headers array, e.g.
 * <pre>
 * IDs:      id-1  | id-2  | id-3  | ... | id-n |
 * Headers:  hdr-1 | hdr-2 | hdr-3 | ... | hdr-n |
 * </pre>
 *
 * The arrays are populated from left to right, and any 0 slots in 'ids' can terminate an interation, or signal empty slots.
 * <br/>
 * It is assumed that we only have a few headers, 3-4 on average. Note that getting a header for a given key and
 * putting a new key/header are operations with O(n) cost, so this implementation is <em>not</em> recommended for
 * a large number of elements.
 * <br/>
 * This class is synchronized for writes (put(), resize()), but not for reads (size(), get())
 * @author Bela Ban
 */
public class Headers {
    private short[]  ids;
    private Header[] hdrs;

    /** Add space for 3 new elements when resizing */
    private static final int RESIZE_INCR=3;

    public Headers(int capacity) {
        init(capacity);
    }

    protected Headers() {}

    public short[] getRawIDs() {
        return ids;
    }

    public Header[] getRawHeaders() {
        return hdrs;
    }

    public synchronized Headers copy() {
        if(ids == null || hdrs == null)
            return new Headers();
        Headers retval=new Headers(ids.length);
        System.arraycopy(ids, 0, retval.ids, 0, ids.length);
        System.arraycopy(hdrs, 0, retval.hdrs, 0, hdrs.length);
        return retval;
    }

    /**
     * Returns the header associated with an ID
     * @param id The ID
     * @return
     */
    public Header getHeader(short id) {
        for(int i=0; i < ids.length; i++) {
            short current_id=ids[i];
            if(current_id == 0)
                return null;
            if(current_id == id)
                return hdrs[i];
        }
        return null;
    }

    public Map<Short,Header> getHeaders() {
        Map<Short,Header> retval=new HashMap<>(ids.length);
        for(int i=0; i < ids.length; i++) {
            if(ids[i] > 0)
                retval.put(ids[i], hdrs[i]);
            else
                break;
        }
        return retval;
    }

    public String printHeaders() {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        for(int i=0; i < ids.length; i++) {
            if(ids[i] > 0) {
                if(first)
                    first=false;
                else
                    sb.append(", ");
                Class clazz=ClassConfigurator.getProtocol(ids[i]);
                String name=clazz != null? clazz.getSimpleName() : Short.toString(ids[i]);
                sb.append(name).append(": ").append(hdrs[i]);
            }
            else
                break;
        }
        return sb.toString();
    }


    /** Puts a header given a key into the hashmap. Overwrites potential existing entry. */
    public void putHeader(short id, Header hdr) {
        _putHeader(id, hdr, 0, true);
    }




    /**
     * Puts a header given a key into the map, only if the key doesn't exist yet
     * @param id
     * @param hdr
     * @return the previous value associated with the specified id, or
     *         <tt>null</tt> if there was no mapping for the id.
     *         (A <tt>null</tt> return can also indicate that the map
     *         previously associated <tt>null</tt> with the id,
     *         if the implementation supports null values.)
     */
    public Header putHeaderIfAbsent(short id, Header hdr) {
        return _putHeader(id, hdr, 0, false);
    }


    public int marshalledSize() {
        int retval=0;
        for(int i=0; i < ids.length; i++) {
            if(ids[i] > 0) {
                retval+=Global.SHORT_SIZE *2;    // for protocol ID and magic number
                retval+=hdrs[i].size();
            }
            else
                break;
        }
        return retval;
    }

    public int size() {
        int retval=0;
        for(int i=0; i < ids.length; i++) {
            if(ids[i] > 0)
                retval++;
            else
                break;
        }
        return retval;
    }

    public int size(short ... excluded_ids) {
         int retval=0;
         for(int i=0; i < ids.length; i++) {
             if(ids[i] > 0) {
                 if(!Util.containsId(ids[i], excluded_ids))
                     retval++;
             }
             else
                 break;
         }
         return retval;
     }

    public int capacity() {
        return ids.length;
    }

    public String printObjectHeaders() {
        StringBuilder sb=new StringBuilder();
        for(int i=0; i < ids.length; i++) {
            if(ids[i] > 0)
                sb.append(ids[i]).append(": ").append(hdrs[i]).append('\n');
            else
                break;
        }
        return sb.toString();
    }

    public String toString() {
        return printHeaders();
    }


    protected void init(int length) {
        ids=new short[length];
        hdrs=new Header[length];
    }

    /**
     * Increases the capacity of the array and copies the contents of the old into the new array
     */
    private void resize() {
        int new_capacity=ids.length + RESIZE_INCR;

        short[] new_ids=new short[new_capacity];
        Header[] new_hdrs=new Header[new_capacity];

        System.arraycopy(ids, 0, new_ids, 0, ids.length);
        System.arraycopy(hdrs, 0, new_hdrs, 0, hdrs.length);

        ids=new_ids;
        hdrs=new_hdrs;
    }


    private synchronized Header _putHeader(short id, Header hdr, int start_index, boolean replace_if_present) {
        int i=start_index;
        while(i < ids.length) {
            if(ids[i] == 0) {
                ids[i]=id;
                hdrs[i]=hdr;
                return null;
            }
            if(ids[i] == id) {
                Header retval=hdrs[i];
                if(replace_if_present || retval == null) {
                    hdrs[i]=hdr;
                }
                return retval;
            }
            i++;
            if(i >= ids.length) {
                resize();
            }
        }
        throw new IllegalStateException("unable to add element " + id + ", index=" + i); // we should never come here
    }


}
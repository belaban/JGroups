package org.jgroups.util;

import org.jgroups.Global;
import org.jgroups.Header;

import java.util.HashMap;
import java.util.Map;

/**
 * Open addressing based implementation of a hashmap (not supporting the Map interface though) for message
 * headers. The keys are strings and the values Headers, and they're stored in an array in the format
 * key-1 | header-1 | key-2 | header-2. The array is populated from left to right, so any null slots can terminate
 * an interation, or signal empty slots.
 * <br/>
 * It is assumed that we only have a few headers, 3-4 on average. Note that getting a header for a given key and
 * putting a new key/header are operations with O(n) cost, so this implementation is <em>not</em> recommended for
 * a large number of elements.
 * <br/>
 * This class is not synchronized
 * @author Bela Ban
 * @version $Id: Headers.java,v 1.12 2008/07/31 08:06:14 belaban Exp $
 */
public class Headers {
    /** Used to store strings and headers, e.g: name-1 | header-1 | name-2 | header-2 | null | null | name-3 | header-3 */
    private Object[] data;

    /** Add space for 3 new elements when resizing */
    private static final int RESIZE_INCR=6;

    public Headers(int initial_capacity) {
        data=new Object[initial_capacity << 1];
    }

    public Headers(Headers hdrs) {
        data=new Object[hdrs.data.length];
        System.arraycopy(hdrs.data, 0, this.data, 0, hdrs.data.length);
    }

    public Object[] getRawData() {
        return data;
    }

    /**
     * Returns the header associated with key
     * @param key
     * @return
     */
    public Header getHeader(String key) {
        for(int i=0; i < data.length; i+=2) {
            if(data[i] == null)
                return null;
            if(data[i].equals(key))
                return (Header)data[i+1];
        }
        return null;
    }

    public Map<String,Header> getHeaders() {
        Map<String,Header> retval=new HashMap<String,Header>(data.length / 2);
        for(int i=0; i < data.length; i+=2) {
            if(data[i] != null)
                retval.put((String)data[i], (Header)data[i+1]);
            else
                break;
        }
        return retval;
    }

    public String printHeaders() {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        for(int i=0; i < data.length; i+=2) {
            if(data[i] != null) {
                if(first)
                    first=false;
                else
                    sb.append(", ");
                sb.append(data[i]).append(": ").append(data[i+1]);
            }
            else
                break;
        }
        return sb.toString();
    }

    public int getNumHeaders() {
        int num=0;
        for(int i=0; i < data.length; i+=2) {
            if(data[i] != null)
                num++;
            else
                break;
        }
        return num;
    }

    /** Puts a header given a key into the hashmap. Overwrites potential existing entry. */
    public void putHeader(String key, Header hdr) {
        _putHeader(key, hdr, 0, true);
    }




    /**
     * Puts a header given a key into the map, only if the key doesn't exist yet
     * @param key
     * @param hdr
     * @return the previous value associated with the specified key, or
     *         <tt>null</tt> if there was no mapping for the key.
     *         (A <tt>null</tt> return can also indicate that the map
     *         previously associated <tt>null</tt> with the key,
     *         if the implementation supports null values.)
     */
    public Header putHeaderIfAbsent(String key, Header hdr) {
        return _putHeader(key, hdr, 0, false);
    }

    /**
     *
     * @param key
     * @return the header assoaicted with key
     * @deprecated Use getHeader() instead. The issue with removing a header is described in
     * http://jira.jboss.com/jira/browse/JGRP-393
     */
    public Header removeHeader(String key) {
        return getHeader(key);
    }

    public Headers copy() {
        return new Headers(this);
    }

    public int marshalledSize() {
        int retval=0;
        for(int i=0; i < data.length; i+=2) {
            if(data[i] != null) {
                retval+=((String)data[i]).length() +2;
                retval+=(Global.SHORT_SIZE *2); // 2 for magic number, 2 for size (short)
                retval+=((Header)data[i+1]).size();
            }
            else
                break;
        }
        return retval;
    }

    public int size() {
        int retval=0;
        for(int i=0; i < data.length; i+=2) {
            if(data[i] != null)
                retval++;
            else
                break;
        }
        return retval;
    }

    public int capacity() {
        return data.length / 2;
    }

    public String printObjectHeaders() {
        StringBuilder sb=new StringBuilder();
        for(int i=0; i < data.length; i+=2) {
            if(data[i] != null)
                sb.append(data[i]).append(": ").append(data[i+1]).append('\n');
            else
                break;
        }
        return sb.toString();
    }

    public String toString() {
        return printHeaders();
    }


    /**
     * Doubles the capacity of the old data array and copies the contents of the old into the new array
     */
    private void resize() {
        int new_size=data.length + RESIZE_INCR;
        Object[] new_data=new Object[new_size];
        System.arraycopy(data, 0, new_data, 0, data.length);
        data=new_data;
    }


    private Header _putHeader(String key, Header hdr, int start_index, boolean replace_if_present) {
        int i=start_index;
        while(i < data.length) {
            if(data[i] == null) {
                data[i]=key;
                data[i+1]=hdr;
                return null;
            }
            if(data[i].equals(key)) {
                Header retval=(Header)data[i+1];
                if(replace_if_present) {
                    data[i+1]=hdr;
                }
                return retval;
            }
            i+=2;
            if(i >= data.length) {
                resize();
            }
        }
        throw new IllegalStateException("unable to add element " + key + ", index=" + i); // we should never come here
    }


}

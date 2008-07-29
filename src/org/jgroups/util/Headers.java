package org.jgroups.util;

import org.jgroups.Global;
import org.jgroups.Header;

import java.util.HashMap;
import java.util.Map;

/**
 * Open addressing based implementation of a hashmap (not supporting the Map interface though) for message
 * headers. The keys are strings and the values Headers, and they're stored in an array in the format
 * key-1 | header-1 | key-2 | header-2.
 * <br/>
 * It is assumed that we only have a few headers, 3-4 on average. Note that getting a header for a given key and
 * putting a new key/header are operations with O(n) cost, so this implementation is <em>not</em> recommended for
 * a large number of elements.
 * <br/>
 * This class is not synchronized
 * @author Bela Ban
 * @version $Id: Headers.java,v 1.1 2008/07/29 15:18:36 belaban Exp $
 */
public class Headers {
    /** Used to store strings and headers, e.g: name-1 | header-1 | name-2 | header-2 | null | null | name-3 | header-3 */
    private Object[] data;

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

    public Map<String,Header> getHeaders() {
        Map<String,Header> retval=new HashMap<String,Header>(data.length / 2);
        for(int i=0; i < data.length; i+=2) {
            if(data[i] != null)
                retval.put((String)data[i], (Header)data[i+1]);
        }
        return retval;
    }

    public String printHeaders() {
        StringBuilder sb=new StringBuilder("[");
        boolean first=true;
        for(int i=0; i < data.length; i+=2) {
            if(data[i] != null) {
                if(first)
                    first=false;
                else
                    sb.append(", ");
                sb.append(data[i]).append(':').append(data[i+1]);
            }
        }
        sb.append("]");
        return sb.toString();
    }

    public int getNumHeaders() {
        int num=0;
        for(int i=0; i < data.length; i+=2) {
            if(data[i] != null)
                num++;
        }
        return num;
    }

    /** Puts a header given a key into the hashmap. Overwrites potential existing entry. */
    public void putHeader(String key, Header hdr) {
        int available=-1;
        for(int i=0; i < data.length; i+=2) {
            if(data[i] == null && available < 0) {
                available=i;
            }
            if(data[i] != null && data[i].equals(key)) {
                data[i+1]=hdr;
                return;
            }
        }
        if(available < 0) {
            resize();
        }
        else {
            data[available]=key;
            data[available + 1]=hdr;
            return;
        }
        for(int i=data.length / 2; i < data.length; i+=2) {
            if(data[i] == null) {
                data[i]=key;
                data[i+1]=hdr;
                return;
            }
        }
        throw new IllegalStateException("didn't find space for element " + key);
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
        for(int i=0; i < data.length; i+=2) {
            if(data[i] != null && data[i].equals(key)) {
                return (Header)data[i+1];
            }
        }

        putHeader(key, hdr);
        return null;
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

    public Header getHeader(String key) {
        for(int i=0; i < data.length; i+=2) {
            if(data[i] != null && data[i].equals(key))
                return (Header)data[i+1];
        }
        return null;
    }

    public Headers copy() {
        return new Headers(this);
    }

    public int size() {
        int retval=0;
        for(int i=0; i < data.length; i+=2) {
            if(data[i] != null) {
                retval+=((String)data[i]).length() +2;
                retval+=(Global.SHORT_SIZE *2); // 2 for magic number, 2 for size (short)
                retval+=((Header)data[i+1]).size();
            }
        }
        return retval;
    }

    public String printObjectHeaders() {
        StringBuilder sb=new StringBuilder();
        for(int i=0; i < data.length; i+=2) {
            sb.append(data[i]).append(": ").append(data[i+1]).append('\n');
        }
        return sb.toString();
    }



    /**
     * Doubles the capacity of the old data array and copies the contents of the old into the new array. This method
     * is synchronized, we probably don't need this as access to a Headers instance is never concurrent !
     */
    private synchronized void resize() {
        int new_size=data.length * 2;
        Object[] new_data=new Object[new_size];
        System.arraycopy(data, 0, new_data, 0, data.length);
        data=new_data;
    }



}

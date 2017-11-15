package org.jgroups.util;

import org.jgroups.BaseMessage;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.conf.ClassConfigurator;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper class providing functions to manipulate the {@link BaseMessage#headers} array. The headers are stored
 * in the array as follows:
 * <pre>
 * Headers:  hdr-1 | hdr-2 | hdr-3 | ... | hdr-n |
 * </pre>
 *
 * The arrays are populated from left to right, and any empty slot in 'headers' can terminate an interation
 * (e.g. a getHeader())
 * <br/>
 * It is assumed that we only have a few headers, 3-4 on average. Note that getting a header for a given key and
 * putting a new key/header are operations with O(n) cost, so this implementation is <em>not</em> recommended for
 * a large number of elements.
 * <br/>
 * This class is synchronized for writes (put(), resize()), but not for reads (size(), get())
 * @author Bela Ban
 */
public final class Headers {
    private static final int RESIZE_INCR=3;

	private Headers() {
		throw new InstantiationError( "Must not instantiate this class" );
	}

    /**
     * Returns the header associated with an ID
     * @param id The ID
     * @return
     */
    public static <T extends Header> T getHeader(final Header[] hdrs, short id) {
        if(hdrs == null)
            return null;
        for(Header hdr: hdrs) {
            if(hdr == null)
                return null;
            if(hdr.getProtId() == id)
                return (T)hdr;
        }
        return null;
    }

    /**
     * Returns the header associated with a set of IDs
     * @param hdrs
     * @param ids The IDs
     * @param <T>
     * @return
     */
    public static <T extends Header> T getHeader(final Header[] hdrs, short ... ids) {
        if(hdrs == null || ids == null || ids.length == 0)
            return null;
        for(Header hdr: hdrs) {
            if(hdr == null)
                return null;
            for(short id: ids)
                if(hdr.getProtId() == id)
                    return (T)hdr;
        }
        return null;
    }


    public static Map<Short,Header> getHeaders(final Header[] hdrs) {
        if(hdrs == null)
            return new HashMap<>();
        Map<Short,Header> retval=new HashMap<>(hdrs.length);
        for(Header hdr: hdrs) {
            if(hdr == null)
                break;
            retval.put(hdr.getProtId(), hdr);
        }
        return retval;
    }

    public static String printHeaders(final Header[] hdrs) {
        if(hdrs == null)
            return "";
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        for(Header hdr: hdrs) {
            if(hdr == null)
                break;
            short id=hdr.getProtId();
            if(first)
                first=false;
            else
                sb.append(", ");
            Class clazz=ClassConfigurator.getProtocol(id);
            String name=clazz != null? clazz.getSimpleName() : Short.toString(id);
            sb.append(name).append(": ").append(hdr);
        }
        return sb.toString();
    }


    /**
     * Adds hdr at the next available slot. If none is available, the headers array passed in will be copied and the copy
     * returned
     * @param headers The headers array
     * @param id The protocol ID of the header
     * @param hdr The header
     * @param replace_if_present Whether or not to overwrite an existing header
     * @return A new copy of headers if the array needed to be expanded, or null otherwise
     */
    public static Header[] putHeader(final Header[] headers, short id, Header hdr, boolean replace_if_present) {
        int i=0;
        Header[] hdrs=headers;
        boolean resized=false;
        while(i < hdrs.length) {
            if(hdrs[i] == null) {
                hdrs[i]=hdr;
                return resized? hdrs: null;
            }
            short hdr_id=hdrs[i].getProtId();
            if(hdr_id == id) {
                if(replace_if_present || hdrs[i] == null)
                    hdrs[i]=hdr;
                return resized? hdrs : null;
            }
            i++;
            if(i >= hdrs.length) {
                hdrs=resize(hdrs);
                resized=true;
            }
        }
        throw new IllegalStateException("unable to add element " + id + ", index=" + i); // we should never come here
    }

    /**
     * Increases the capacity of the array and copies the contents of the old into the new array
     */
    public static Header[] resize(final Header[] headers) {
        int new_capacity=headers.length + RESIZE_INCR;
        Header[] new_hdrs=new Header[new_capacity];
        System.arraycopy(headers, 0, new_hdrs, 0, headers.length);
        return new_hdrs;
    }

    public static Header[] copy(final Header[] headers) {
        if(headers == null)
            return new Header[0];
        Header[] retval=new Header[headers.length];
        System.arraycopy(headers, 0, retval, 0, headers.length);
        return retval;
    }

    public static int marshalledSize(final Header[] hdrs) {
        int retval=0;
        if(hdrs == null)
            return retval;
        for(Header hdr: hdrs) {
            if(hdr == null)
                break;
            retval+=Global.SHORT_SIZE *2;    // for protocol ID and magic number
            retval+=hdr.serializedSize();
        }
        return retval;
    }

    public static int size(Header[] hdrs) {
        int retval=0;
        if(hdrs == null)
            return retval;
        for(Header hdr: hdrs) {
            if(hdr == null)
                break;
            retval++;
        }
        return retval;
    }

    public static int size(Header[] hdrs, short... excluded_ids) {
        int retval=0;
        if(hdrs == null)
            return retval;
        for(Header hdr: hdrs) {
            if(hdr == null)
                break;
            if(!Util.containsId(hdr.getProtId(), excluded_ids))
                retval++;
        }
        return retval;
    }


}

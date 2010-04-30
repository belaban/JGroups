package org.jgroups.blocks;

import org.jgroups.Message;
import org.jgroups.Address;
import org.jgroups.util.Util;

import java.util.*;

/** Class which captures a bunch of options relevant to remote method invocation or message sending
 * @author Bela Ban
 * @version $Id: RequestOptions.java,v 1.10 2010/04/30 07:50:36 belaban Exp $
 * @since 2.10
 */
public class RequestOptions {
    /** The mode of a request. Defined in GroupRequest e.g. GET_NONE, GET_ALL */
    private int       mode=Request.GET_NONE;

    /** The max time (in ms) for a blocking call. 0 blocks until all responses have been received (if mode = GET_ALL) */
    private long      timeout; // used when mode != GET_NONE

    /** Turns on anycasting; this results in multiple unicasts rather than a multicast for group calls */
    private boolean   use_anycasting;

    /** Allows for filtering of responses */
    private RspFilter rsp_filter;

    /** The scope of a message, allows for concurrent delivery of messages from the same sender */
    private short     scope;

    /** The flags set in the message in which a request is sent */
    private byte      flags; // Message.OOB, Message.DONT_BUNDLE etc

    /** A list of members which should be excluded from a call */
    private Set<Address> exclusion_list;


    public static final RequestOptions SYNC=new RequestOptions(Request.GET_ALL, 5000);
    public static final RequestOptions ASYNC=new RequestOptions(Request.GET_NONE, 5000);


    public RequestOptions() {
    }

    public RequestOptions(int mode, long timeout, boolean use_anycasting, RspFilter rsp_filter, byte flags) {
        this.mode=mode;
        this.timeout=timeout;
        this.use_anycasting=use_anycasting;
        this.rsp_filter=rsp_filter;
        this.flags=flags;
    }

    public RequestOptions(int mode, long timeout, boolean use_anycasting, RspFilter rsp_filter) {
        this(mode, timeout, use_anycasting, rsp_filter, (byte)0);
    }

    public RequestOptions(int mode, long timeout) {
        this(mode, timeout, false, null);
    }


    public int getMode() {
        return mode;
    }

    public RequestOptions setMode(int mode) {
        this.mode=mode;
        return this;
    }

    public long getTimeout() {
        return timeout;
    }

    public RequestOptions setTimeout(long timeout) {
        this.timeout=timeout;
        return this;
    }

    public boolean getAnycasting() {
        return use_anycasting;
    }

    public RequestOptions setAnycasting(boolean use_anycasting) {
        this.use_anycasting=use_anycasting;
        return this;
    }

    public short getScope() {
        return scope;
    }

    public RequestOptions setScope(short scope) {
        this.scope=scope;
        return this;
    }

    public RspFilter getRspFilter() {
        return rsp_filter;
    }

    public RequestOptions setRspFilter(RspFilter rsp_filter) {
        this.rsp_filter=rsp_filter;
        return this;
    }

    public byte getFlags() {
        return flags;
    }

    public RequestOptions setFlags(byte flags) {
        this.flags=Util.setFlag(this.flags, flags);
        return this;
    }

    public RequestOptions clearFlags(byte flags) {
        this.flags=Util.clearFlags(this.flags, flags);
        return this;
    }

    public boolean hasExclusionList() {
        return exclusion_list != null && !exclusion_list.isEmpty();
    }

    public Collection<Address> getExclusionList() {
        if(exclusion_list == null)
            return exclusion_list;
        else
            return Collections.unmodifiableCollection(exclusion_list);
    }

    public RequestOptions setExclusionList(Address ... mbrs) {
        if(exclusion_list == null)
            exclusion_list=new HashSet<Address>();
        else
            exclusion_list.clear();
        exclusion_list.addAll(Arrays.asList(mbrs));
        return this;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("mode=" + Request.modeToString(mode));
        sb.append(", timeout=" + timeout);
        if(use_anycasting)
            sb.append(", anycasting=true");
        sb.append(", flags=" + Message.flagsToString(flags));
        if(scope > 0)
            sb.append(", scope=" + scope);
        if(exclusion_list != null)
            sb.append(", exclusion list: " + Util.print(exclusion_list));
        return sb.toString();
    }
}

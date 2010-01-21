package org.jgroups.blocks;

import org.jgroups.Message;
import org.jgroups.util.Util;

/** Class which captures a bunch of options relevant to remote method invocation or message sending
 * @author Bela Ban
 * @version $Id: RequestOptions.java,v 1.6 2010/01/21 13:09:55 belaban Exp $
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

    /** The flags set in the message in which a request is sent */
    private byte      flags; // Message.OOB, Message.DONT_BUNDLE etc



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


    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("mode=" + Request.modeToString(mode));
        sb.append(", timeout=" + timeout);
        if(use_anycasting)
            sb.append(", anycasting=true");
        sb.append(", flags=" + Message.flagsToString(flags));
        return sb.toString();
    }
}

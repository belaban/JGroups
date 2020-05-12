
package org.jgroups;


import org.jgroups.conf.ClassConfigurator;
import org.jgroups.util.Headers;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * A common superclass for all {@link Message} implementations. It contains functionality to manage headers, flags and
 * destination and source addresses.
 *
 * @since  5.0
 * @author Bela Ban
 */
public abstract class BaseMessage implements Message {
    protected Address           dest;
    protected Address           sender;
    protected volatile Header[] headers;
    protected volatile short    flags;
    protected volatile byte     transient_flags; // transient_flags is neither marshalled nor copied

    static final byte           DEST_SET         =  1;
    static final byte           SRC_SET          =  1 << 1;


    public BaseMessage() {
    }

    /**
    * Constructs a message given a destination address
    * @param dest The Address of the receiver. If it is null, then the message is sent to the group. Otherwise, it is
    *             sent to a single member.
    */
    public BaseMessage(Address dest) {
        setDest(dest);
        headers=createHeaders(Util.DEFAULT_HEADERS);
    }


    public Address               getDest()                 {return dest;}
    public Message               setDest(Address new_dest) {dest=new_dest; return this;}
    public Address               getSrc()                  {return sender;}
    public Message               setSrc(Address new_src)   {sender=new_src; return this;}
    public int                   getNumHeaders()           {return Headers.size(this.headers);}
    public Map<Short,Header>     getHeaders()              {return Headers.getHeaders(this.headers);}
    public String                printHeaders()            {return Headers.printHeaders(this.headers);}


    /**
     * Sets a number of flags in a message
     * @param flags The flag or flags
     * @return A reference to the message
     */
    public Message setFlag(Flag... flags) {
        if(flags != null) {
            short tmp=this.flags;
            for(Flag flag : flags) {
                if(flag != null)
                    tmp|=flag.value();
            }
            this.flags=tmp;
        }
        return this;
    }

    /**
     * Same as {@link #setFlag(Flag...)} except that transient flags are not marshalled
     * @param flags The flag
     */
    public Message setFlag(TransientFlag... flags) {
        if(flags != null) {
            short tmp=this.transient_flags;
            for(TransientFlag flag : flags)
                if(flag != null)
                    tmp|=flag.value();
            this.transient_flags=(byte)tmp;
        }
        return this;
    }


    public Message setFlag(short flag, boolean transient_flags) {
        short tmp=transient_flags? this.transient_flags : this.flags;
        tmp|=flag;
        if(transient_flags)
            this.transient_flags=(byte)tmp;
        else
            this.flags=tmp;
        return this;
    }


    /**
     * Returns the internal representation of flags. Don't use this, as the internal format might change at any time !
     * This is only used by unit test code
     * @return
     */
    public short getFlags(boolean transient_flags) {return transient_flags? this.transient_flags : flags;}

    /**
     * Clears a number of flags in a message
     * @param flags The flags
     * @return A reference to the message
     */
    public Message clearFlag(Flag... flags) {
        if(flags != null) {
            short tmp=this.flags;
            for(Flag flag : flags)
                if(flag != null)
                    tmp&=~flag.value();
            this.flags=tmp;
        }
        return this;
    }

    public Message clearFlag(TransientFlag... flags) {
        if(flags != null) {
            short tmp=this.transient_flags;
            for(TransientFlag flag : flags)
                if(flag != null)
                    tmp&=~flag.value();
            this.transient_flags=(byte)tmp;
        }
        return this;
    }

    /**
     * Checks if a given flag is set
     * @param flag The flag
     * @return Whether or not the flag is currently set
     */
    public boolean isFlagSet(Flag flag) {
        return Util.isFlagSet(flags, flag);
    }

    public boolean isFlagSet(TransientFlag flag) {
        return Util.isTransientFlagSet(transient_flags, flag);
    }

    /**
    * Atomically checks if a given flag is set and - if not - sets it. When multiple threads
    * concurrently call this method with the same flag, only one of them will be able to set the
    * flag
    *
    * @param flag
    * @return True if the flag could be set, false if not (was already set)
    */
    public synchronized boolean setFlagIfAbsent(TransientFlag flag) {
        if(isFlagSet(flag))
            return false;
        setFlag(flag);
        return true;
    }

    /**
     * Copies the source- and destination addresses, flags and headers (if copy_headers is true).<br/>
     * If copy_payload is set, then method {@link #copyPayload(Message)} of the subclass will be called, which is
     * responsible for copying the payload specific to that message type.<br/>
     * Note that for headers, only the arrays holding references to the headers are copied, not the headers themselves !
     * The consequence is that the headers array of the copy hold the *same* references as the original, so do *not*
     * modify the headers ! If you want to change a header, copy it and call {@link Message#putHeader(short,Header)} again.
     */
    public Message copy(boolean copy_payload, boolean copy_headers) {
        BaseMessage retval=(BaseMessage)create().get();
        retval.dest=dest;
        retval.sender=sender;
        retval.flags=this.flags;
        retval.transient_flags=this.transient_flags;
        retval.headers=copy_headers && headers != null? Headers.copy(this.headers) : createHeaders(Util.DEFAULT_HEADERS);
        if(copy_payload)
            copyPayload(retval);
        return retval;
    }


    /** Puts a header given an ID into the hashmap. Overwrites potential existing entry. */
    public Message putHeader(short id, Header hdr) {
        if(id < 0)
            throw new IllegalArgumentException("An ID of " + id + " is invalid");
        if(hdr != null)
            hdr.setProtId(id);
        synchronized(this) {
            if(this.headers == null)
                this.headers=createHeaders(Util.DEFAULT_HEADERS);
            Header[] resized_array=Headers.putHeader(this.headers, id, hdr, true);
            if(resized_array != null)
                this.headers=resized_array;
        }
        return this;
    }



    public <T extends Header> T getHeader(short id) {
        if(id <= 0)
            throw new IllegalArgumentException("An ID of " + id + " is invalid. Add the protocol which calls " +
                                                 "getHeader() to jg-protocol-ids.xml");
        return Headers.getHeader(this.headers, id);
    }

    public <T> T   getPayload() {return getObject();}

    public Message setPayload(Object pl) {return setObject(pl);}

    public String toString() {
        return String.format("[%s to %s, %d bytes%s%s]", sender, dest == null? "<all>" : dest,
                             getLength(), flags > 0? ", flags=" + Util.flagsToString(flags) : "",
                             transient_flags > 0? ", transient_flags=" + Util.transientFlagsToString(transient_flags) : "");
    }

    public int serializedSize() {
        return size();
    }

    public int size() {
        int retval=Global.BYTE_SIZE // leading byte
          + Global.SHORT_SIZE;      // flags
        if(dest != null)
            retval+=Util.size(dest);
        if(sender != null)
            retval+=Util.size(sender);

        retval+=Global.SHORT_SIZE;  // number of headers
        retval+=Headers.marshalledSize(this.headers);
        return retval;
    }

    public void writeTo(DataOutput out) throws IOException {
        byte leading=0;

        if(dest != null)
            leading=Util.setFlag(leading, DEST_SET);

        if(sender != null)
            leading=Util.setFlag(leading, SRC_SET);

        // write the leading byte first
        out.write(leading);

        // write the flags (e.g. OOB, LOW_PRIO), skip the transient flags
        out.writeShort(flags);

        // write the dest_addr
        if(dest != null)
            Util.writeAddress(dest, out);

        // write the src_addr
        if(sender != null)
            Util.writeAddress(sender, out);

        // write the headers
        writeHeaders(this.headers, out, (short[])null);

        // finally write the payload
        writePayload(out);
    }

    public void writeToNoAddrs(Address src, DataOutput out, short... excluded_headers) throws IOException {
        byte leading=0;

        boolean write_src_addr=src == null || sender != null && !sender.equals(src);

        if(write_src_addr)
            leading=Util.setFlag(leading, SRC_SET);

        // write the leading byte first
        out.write(leading);

        // write the flags (e.g. OOB, LOW_PRIO)
        out.writeShort(flags);

        // write the src_addr
        if(write_src_addr)
            Util.writeAddress(sender, out);

        // write the headers
        writeHeaders(this.headers, out, excluded_headers);

        // finally write the payload
        writePayload(out);
    }


    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        // 1. read the leading byte first
        byte leading=in.readByte();

        // 2. the flags
        flags=in.readShort();

        // 3. dest_addr
        if(Util.isFlagSet(leading, DEST_SET))
            dest=Util.readAddress(in);

        // 4. src_addr
        if(Util.isFlagSet(leading, SRC_SET))
            sender=Util.readAddress(in);

        // 5. headers
        int len=in.readShort();
        if(this.headers == null || len > this.headers.length)
            this.headers=createHeaders(len);
        for(int i=0; i < len; i++) {
            short id=in.readShort();
            Header hdr=readHeader(in).setProtId(id);
            this.headers[i]=hdr;
        }
        readPayload(in);
    }


    /** Copies the payload */
    protected Message copyPayload(Message copy) {
        return copy;
    }

    protected static void writeHeaders(Header[] hdrs, DataOutput out, short ... excluded_headers) throws IOException {
        int size=Headers.size(hdrs, excluded_headers);
        out.writeShort(size);
        if(size > 0) {
            for(Header hdr : hdrs) {
                if(hdr == null)
                    break;
                short id=hdr.getProtId();
                if(Util.containsId(id, excluded_headers))
                    continue;
                out.writeShort(id);
                writeHeader(hdr, out);
            }
        }
    }

    protected static void writeHeader(Header hdr, DataOutput out) throws IOException {
        short magic_number=hdr.getMagicId();
        out.writeShort(magic_number);
        hdr.writeTo(out);
    }

    protected static Header readHeader(DataInput in) throws IOException, ClassNotFoundException {
        short magic_number=in.readShort();
        Header hdr=ClassConfigurator.create(magic_number);
        hdr.readFrom(in);
        return hdr;
    }

    protected static Header[] createHeaders(int size) {
        return size > 0? new Header[size] : new Header[3];
    }


}

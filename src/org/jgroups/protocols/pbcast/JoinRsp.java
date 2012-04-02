
package org.jgroups.protocols.pbcast;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.util.Digest;
import org.jgroups.util.MutableDigest;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.List;


/**
 * Result of a JOIN request (sent by the GMS client). Instances of this class are immutable.
 */
public class JoinRsp implements Streamable {
    private View    view=null;
    private Digest  digest=null;
    /** only set if JOIN failed, e.g. in AUTH */
    private String  fail_reason=null;

    protected static final byte VIEW_PRESENT        = 1 << 0;
    protected static final byte DIGEST_PRESENT      = 1 << 1;
    protected static final byte FAIL_REASON_PRESENT = 1 << 2;


    public JoinRsp() {

    }

    public JoinRsp(View v, Digest d) {
        view=v;
        digest=d;
    }

    public JoinRsp(String fail_reason) {
        this.fail_reason=fail_reason;
    }

    public View getView() {
        return view;
    }

    public Digest getDigest() {
        return digest;
    }

    public String getFailReason() {
        return fail_reason;
    }

    public void setFailReason(String r) {
        fail_reason=r;
    }


    public void writeTo(DataOutput out) throws Exception {
        byte flags=0;
        if(view != null)
            flags|=VIEW_PRESENT;
        if(digest != null)
            flags|=DIGEST_PRESENT;
        if(fail_reason != null)
            flags|=FAIL_REASON_PRESENT;
        out.writeByte(flags);

        // 1. View: viewId
        int size=0;
        if(view != null) {
            view.getViewId().writeTo(out);
            size=view.size();

            // 2. View: members
            out.writeShort(size); // we'll never have more than a couple of 1000 members !
            for(Address mbr: view)
                Util.writeAddress(mbr, out);
        }

        // 3. Digest
        if(digest != null && view != null) { // if digest is present, view *has* to be present !
            for(Address mbr: view) {
                long[] seqnos=digest.get(mbr);
                if(seqnos == null)
                    seqnos=new long[]{-1, -1};
                Util.writeLongSequence(seqnos[0], seqnos[1], out);
            }
        }

        // 4. fail_reason
        if(fail_reason != null)
            out.writeUTF(fail_reason);
    }

    public void readFrom(DataInput in) throws Exception {
        byte flags=in.readByte();

        int size=0;
        List<Address> members=null;
        if((flags & VIEW_PRESENT) == VIEW_PRESENT) {
            // 1. View: viewId
            ViewId vid=new ViewId();
            vid.readFrom(in);

            // 2. View: members
            size=in.readShort();
            members=new ArrayList<Address>(size);
            for(int i=0; i < size; i++) {
                Address mbr=Util.readAddress(in);
                members.add(mbr);
            }

            view=new View(vid, members);
        }

        // 3. Digest
        if(members != null && (flags & DIGEST_PRESENT) == DIGEST_PRESENT) {
            MutableDigest tmp=new MutableDigest(size);
            for(int i=0; i < size; i++) {
                Address mbr=members.get(i);
                long[] seqnos=Util.readLongSequence(in);
                if(seqnos[0] == -1 && seqnos[1] == -1)
                    continue;
                tmp.add(mbr, seqnos[0], seqnos[1], false);
            }
            digest=tmp;
        }

        // 4. fail_reason
        if((flags & FAIL_REASON_PRESENT) == FAIL_REASON_PRESENT)
            fail_reason=in.readUTF();
    }

    public int serializedSize() {
        int retval=Global.BYTE_SIZE; // 'flags' byte

        if(view != null) {
            retval+=view.getVid().serializedSize();

            retval+=Global.SHORT_SIZE; // size
            for(Address mbr: view)
                retval+=Util.size(mbr);
        }
        
        if(digest != null) {
            for(Digest.DigestEntry entry: digest)
                retval+=Util.size(entry.getHighestDeliveredSeqno(), entry.getHighestReceivedSeqno());
        }
        
        if(fail_reason != null)
            retval+=fail_reason.length() +2;
        return retval;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("view: ");
        if(view == null)
            sb.append("<null>");
        else
            sb.append(view);
        sb.append(", digest: ");
        if(digest == null)
            sb.append("<null>");
        else
            sb.append(digest);
        if(fail_reason != null)
            sb.append(", fail reason: ").append(fail_reason);
        return sb.toString();
    }
}

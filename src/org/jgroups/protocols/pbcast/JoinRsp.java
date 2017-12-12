
package org.jgroups.protocols.pbcast;


import org.jgroups.Constructable;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.util.Digest;
import org.jgroups.util.SizeStreamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;


/**
 * Result of a JOIN request (sent by the GMS client). Instances of this class are immutable.
 */
public class JoinRsp implements SizeStreamable, Constructable<JoinRsp> {
    protected View    view;
    protected Digest  digest;
    protected String  fail_reason; /** only set if JOIN failed, e.g. in AUTH */

    protected static final byte VIEW_PRESENT        = 1 << 0;
    protected static final byte DIGEST_PRESENT      = 1 << 1;
    protected static final byte FAIL_REASON_PRESENT = 1 << 2;


    public JoinRsp() {
    }

    public JoinRsp(View v, Digest d) {
        view=v;
        digest=d;
    }

    public JoinRsp(String fail_reason) {this.fail_reason=fail_reason;}

    public Supplier<? extends JoinRsp> create() {
        return JoinRsp::new;
    }

    public View    getView()               {return view;}
    public Digest  getDigest()             {return digest;}
    public String  getFailReason()         {return fail_reason;}
    public JoinRsp setFailReason(String r) {fail_reason=r; return this;}

    @Override
    public void writeTo(DataOutput out) throws IOException {
        byte flags=0;
        if(view != null)
            flags|=VIEW_PRESENT;
        if(digest != null)
            flags|=DIGEST_PRESENT;
        if(fail_reason != null)
            flags|=FAIL_REASON_PRESENT;
        out.writeByte(flags);

        // 1. view
        if(view != null)
            view.writeTo(out);

        // 2. digest
        if(digest != null)
            digest.writeTo(out, false); // don't write the membership; it is already present in the view

        // 3. fail_reason
        if(fail_reason != null)
            out.writeUTF(fail_reason);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        byte flags=in.readByte();

        // 1. view
        if((flags & VIEW_PRESENT) == VIEW_PRESENT) {
            view=new View();
            view.readFrom(in);
        }

        // 2. digest
        if((flags & DIGEST_PRESENT) == DIGEST_PRESENT) {
            digest=new Digest(view.getMembersRaw());
            digest.readFrom(in, false);
        }

        // 3. fail_reason
        if((flags & FAIL_REASON_PRESENT) == FAIL_REASON_PRESENT)
            fail_reason=in.readUTF();
    }

    @Override
    public int serializedSize() {
        int retval=Global.BYTE_SIZE; // flags

        if(view != null)
            retval+=view.serializedSize();

        if(digest != null)
            retval+=digest.serializedSize(false);

        if(fail_reason != null)
            retval+=fail_reason.length() +2;
        return retval;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        if(view != null)
            sb.append("view: ").append(view);
        if(digest != null)
            sb.append(", digest: ").append(digest);
        if(fail_reason != null)
            sb.append("fail reason: ").append(fail_reason);
        return sb.toString();
    }
}

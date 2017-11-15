
package org.jgroups;

import org.jgroups.util.SizeStreamable;

/**
 * Header is a JGroups internal base class for all JGroups headers. Client normally do not need to
 * interact with headers unless they are developing their custom protocols.
 * 
 * @author Bela Ban
 * @since 2.0
 */
public abstract class Header implements SizeStreamable, Constructable<Header> {
    /** The ID of the protocol which added a header to a message. Set externally, e.g. by {@link BaseMessage#putHeader(short,Header)} */
    protected short prot_id;

    public short  getProtId()         {return prot_id;}
    public Header setProtId(short id) {this.prot_id=id; return this;}

    /** Returns the magic-ID. If defined in jg-magic-map.xml, the IDs need to be the same */
    public abstract short getMagicId();

    /** @deprecated Headers should implement {@link SizeStreamable#serializedSize()} instead */
    @Deprecated public int size() {
        return serializedSize();
    }

    public String toString() {
        return '[' + getClass().getSimpleName() + "]";
    }

}

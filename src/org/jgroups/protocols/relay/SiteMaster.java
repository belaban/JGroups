package org.jgroups.protocols.relay;

/**
 * Special address with the UUID part being 0: identifies the current (relay) coordinator of a given site. E,g, if we
 * send a message with dest=SiteMaster(SFO) from site LON, then the message will be forwarded to the relay coordinator
 * of the SFO site
 * @author Bela Ban
 * @since 3.2
 */
public class SiteMaster extends SiteUUID {
    private static final long serialVersionUID=-1110144992073882353L;

    public SiteMaster() {
    }

    public SiteMaster(short site) {
        super(0, 0, site);
    }
}

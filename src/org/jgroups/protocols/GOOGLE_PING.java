package org.jgroups.protocols;

import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Property;

/**
 * Discovery protocol for Google Cloud Storage. Very simple first shot at an impl, based on a simple migration of
 * S3_PING, as discussed in [1].<p/>
 * The location property needs to be the bucket name.<p/>
 * [1] https://developers.google.com/storage/docs/migrating#migration-simple
 * @author Bela Ban
 * @since 3.5
 */
@Experimental
public class GOOGLE_PING extends S3_PING {

    @Property(description="The name of the Google Cloud Storage server")
    protected String host="storage.googleapis.com";

    public void init() throws Exception {
        super.init();
    }

    protected AWSAuthConnection createConnection() {
        return new AWSAuthConnection(access_key, secret_access_key, false, host);
    }
}






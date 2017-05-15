package org.jgroups.protocols;

/**
 * Discovery protocol for Google Cloud Storage. Very simple first shot at an impl, based on a simple migration of
 * S3_PING, as discussed in [1].<p/>
 * The location property needs to be the bucket name.<p/>
 * [1] https://developers.google.com/storage/docs/migrating#migration-simple
 * @author Bela Ban
 * @since 3.5
 * @deprecated Use GOOGLE_PING2 instead: https://github.com/jgroups-extras/jgroups-google
 */
@Deprecated
public class GOOGLE_PING extends S3_PING {

    public void init() throws Exception {
        if(host == null)
            host="storage.googleapis.com";
        super.init();
    }

    protected AWSAuthConnection createConnection() {
       // Fix for JGRP-1992. Always use secure port, if port is not specified
        int tmp_port=port > 0? port : Utils.SECURE_PORT;
        return new AWSAuthConnection(access_key, secret_access_key, use_ssl, host, tmp_port);
    }
}






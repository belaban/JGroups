package org.jgroups.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.ChannelException;

import java.io.IOException;
import java.io.ObjectStreamClass;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;

/**
 * Uses magic numbers for class descriptors
 * @author Bela Ban
 * @version $Id: MagicObjectInputStream.java,v 1.3 2004/10/04 20:43:34 belaban Exp $
 */
public class MagicObjectInputStream extends ContextObjectInputStream {
    static ClassConfigurator conf=null;
    static final Log log=LogFactory.getLog(MagicObjectInputStream.class);


    public MagicObjectInputStream(InputStream is) throws IOException {
        super(is);
        if(conf == null) {
            try {
                conf=ClassConfigurator.getInstance(false);
            }
            catch(ChannelException e) {
                log.error("ClassConfigurator could not be instantiated", e);
            }
        }
    }


    protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
        ObjectStreamClass retval;
        int magic_num=super.readInt();

        if(conf == null || magic_num == -1) {
            return super.readClassDescriptor();
        }

        retval=conf.getObjectStreamClassFromMagicNumber(magic_num);
        if(retval == null)
            throw new ClassNotFoundException("failed fetching class descriptor for magic number " + magic_num);
        //if(log.isTraceEnabled())
            //log.trace("reading descriptor (from " + magic_num + "): " + retval.getName());
        return retval;
    }
}


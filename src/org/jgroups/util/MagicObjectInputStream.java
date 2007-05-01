package org.jgroups.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.ChannelException;
import org.jgroups.conf.ClassConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectStreamClass;

/**
 * Uses magic numbers for class descriptors
 * @author Bela Ban
 * @version $Id: MagicObjectInputStream.java,v 1.6 2007/05/01 09:15:17 belaban Exp $
 */
public class MagicObjectInputStream extends ContextObjectInputStream {
    static volatile ClassConfigurator conf=null;
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
        short magic_num=super.readShort();

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


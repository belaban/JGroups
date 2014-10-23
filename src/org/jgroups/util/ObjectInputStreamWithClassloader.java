package org.jgroups.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * Override {@link java.io.ObjectInputStream#resolveClass(java.io.ObjectStreamClass)} using the passed-in
 * classloader
 * @author Bela Ban
 * @since  3.5
 */
public class ObjectInputStreamWithClassloader extends ObjectInputStream {
    protected final ClassLoader loader;

    public ObjectInputStreamWithClassloader(InputStream in) throws IOException {
        this(in, null);
    }

    public ObjectInputStreamWithClassloader(InputStream in, ClassLoader loader) throws IOException {
        super(in);
        this.loader=loader;
    }

    protected ObjectInputStreamWithClassloader() throws IOException, SecurityException {
        this((ClassLoader)null);
    }

    protected ObjectInputStreamWithClassloader(ClassLoader loader) throws IOException, SecurityException {
        this.loader=loader;
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
        if(loader == null)
            return super.resolveClass(desc);

        String name=desc.getName();
        try {
            return Class.forName(name, false, loader);
        }
        catch (ClassNotFoundException ex) {
            Class<?> cl=super.resolveClass(desc);
            if(cl != null)
                return cl;
            throw ex;
        }
    }
}

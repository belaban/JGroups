package org.jgroups.util;

import org.jgroups.Global;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Method;

/**
 * Override {@link java.io.ObjectInputStream#resolveClass(java.io.ObjectStreamClass)} using the passed-in
 * classloader.
 * <p/>
 * This is the single point through which all JGroups Java-serialization based deserialization goes, both for
 * RPC calls (e.g. {@link org.jgroups.blocks.MethodCall} arguments/return values, via {@link Util#objectFromStream}
 * and {@link Util#objectFromByteBuffer(byte[])}) and for state-transfer / replicated-map marshalling (e.g.
 * {@link org.jgroups.blocks.ReplicatedHashMap}, {@link org.jgroups.blocks.PartitionedHashMap}). To harden this
 * against unsafe deserialization (gadget-chain) attacks, a {@code java.io.ObjectInputFilter} (JEP 290) can be
 * configured process-wide via the {@link Global#DESERIALIZATION_FILTER} system property; it is then applied to
 * every stream created here.
 * <p/>
 * {@code java.io.ObjectInputFilter} only exists since Java 9, but this module still targets Java 8, so the API
 * is accessed via reflection (same technique already used for e.g. Loom fibers in {@link Util#createFiber}): on
 * a JVM that doesn't support it, {@link #FILTER} stays {@code null} and filtering is simply skipped. Like every
 * other {@code jgroups.*} system property in this codebase (see the static initializer in {@link Util}), the
 * property is read once, when this class is loaded - it's not re-checked per stream. To change it, set it on the
 * command line (or otherwise before JGroups classes are loaded), not at runtime.
 * @author Bela Ban
 * @since  3.5
 */
public class ObjectInputStreamWithClassloader extends ObjectInputStream {
    protected static final Log    log=LogFactory.getLog(ObjectInputStreamWithClassloader.class);

    // ObjectInputStream.setObjectInputFilter(ObjectInputFilter) and the filter instance built from
    // Global.DESERIALIZATION_FILTER; both stay null when the property is unset or the JVM doesn't support it
    //
    // WHY REFLECTION: java.io.ObjectInputFilter (and ObjectInputStream.setObjectInputFilter()) were only added
    // in Java 9 (JEP 290). This module still targets Java 8 (see maven.compiler.source/target in pom.xml), so
    // ObjectInputStreamWithClassloader - loaded on every single deserialization, RPC and state alike - cannot
    // reference those types/methods directly: doing so would make this class fail to link (NoClassDefFoundError/
    // NoSuchMethodError) the moment it's loaded on a Java 8 JVM, breaking JGroups entirely for Java 8 users, even
    // if they never set jgroups.deserialization.filter. Going through Class.forName()/getMethod() defers that
    // lookup to runtime: on Java 8 it simply throws (caught below), FILTER/SET_FILTER stay null, and filtering is
    // skipped - everything else keeps working exactly as before. On Java 9+, it resolves normally and the filter
    // is applied. This mirrors the same pattern already used elsewhere in this codebase for optional, JDK-version
    // -gated APIs, e.g. the Loom/virtual-thread support in Util#createFiber().
    protected static final Method SET_FILTER;
    protected static final Object FILTER;

    static {
        Method set_filter=null;
        Object filter=null;
        String pattern=System.getProperty(Global.DESERIALIZATION_FILTER);
        if(pattern != null && !(pattern=pattern.trim()).isEmpty()) {
            try {
                // reflection instead of "import java.io.ObjectInputFilter": see the "WHY REFLECTION" note above -
                // this whole try block would just be "ObjectInputFilter.Config.createFilter(pattern)" plus a
                // direct call to setObjectInputFilter() if this module didn't still need to run on Java 8
                Class<?> filter_class=Class.forName("java.io.ObjectInputFilter");
                Class<?> config_class=Class.forName("java.io.ObjectInputFilter$Config");
                filter=config_class.getMethod("createFilter", String.class).invoke(null, pattern);
                set_filter=ObjectInputStream.class.getMethod("setObjectInputFilter", filter_class);
            }
            catch(Exception ex) {
                // most commonly ClassNotFoundException/NoSuchMethodException on Java 8 (expected, not a bug);
                // could also be an invalid pattern (IllegalArgumentException wrapped in InvocationTargetException)
                log.warn("failed creating ObjectInputFilter from %s=\"%s\" (deserialization will NOT be " +
                           "filtered; this requires Java 9+): %s", Global.DESERIALIZATION_FILTER, pattern, ex);
                filter=null;
                set_filter=null;
            }
        }
        FILTER=filter;
        SET_FILTER=set_filter;
    }

    protected final ClassLoader loader;

    public ObjectInputStreamWithClassloader(InputStream in) throws IOException {
        this(in, null);
    }

    public ObjectInputStreamWithClassloader(InputStream in, ClassLoader loader) throws IOException {
        super(in);
        this.loader=loader;
        applyFilter();
    }

    protected ObjectInputStreamWithClassloader() throws IOException, SecurityException {
        this((ClassLoader)null);
    }

    protected ObjectInputStreamWithClassloader(ClassLoader loader) throws IOException, SecurityException {
        this.loader=loader;
        applyFilter();
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

    /**
     * Applies the {@link #FILTER} built above to {@code this} stream, i.e. the reflective equivalent of
     * {@code this.setObjectInputFilter(FILTER)}. {@link #SET_FILTER} is only non-null on a JVM that actually
     * has {@code ObjectInputStream.setObjectInputFilter()} (Java 9+) and a valid pattern was configured; on
     * Java 8, or when the property is unset, this is a no-op.
     */
    protected void applyFilter() {
        if(SET_FILTER == null)
            return;
        try {
            SET_FILTER.invoke(this, FILTER);
        }
        catch(Exception ex) {
            // don't fail deserialization over a problem applying the filter itself
            log.warn("failed applying ObjectInputFilter: %s", ex);
        }
    }
}

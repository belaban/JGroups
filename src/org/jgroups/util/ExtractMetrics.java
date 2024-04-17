package org.jgroups.util;

import org.jgroups.JChannel;
import org.jgroups.jmx.ResourceDMBean;
import org.jgroups.stack.Protocol;

import javax.management.MBeanAttributeInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Extracts all attributes and methods annotated with {@link org.jgroups.annotations.ManagedAttribute} and returns them
 * as a map of names associated with [getter-method/description tuples]. E.g. for an attribute called foo, a method
 * foo() or getFoo() is searched for.
 * @author Bela Ban
 * @since  5.4, 5.3.6
 */
public class ExtractMetrics {
    protected JChannel ch;

    public static class Entry {
        protected final String description;
        protected final Supplier<Object> method;

        protected Entry(String description, Supplier<Object> method) {
            this.description=description;
            this.method=method;
        }

        public String getDescription() {
            return description;
        }

        public Supplier<Object> getMethod() {
            return method;
        }

        @Override
        public String toString() {
            return String.format("  %s [%s]", method.get(), description);
        }
    }

    public static Map<String,Map<String,Entry>> extract(JChannel ch) {
        Map<String,Map<String,Entry>> map=new HashMap<>();
        for(Protocol p: ch.stack().getProtocols()) {
            map.put(p.getName(), extract(p));
        }
        return map;
    }


    public static Map<String,Entry> extract(Protocol p) {
        Map<String,Entry> map=new HashMap<>();

        ResourceDMBean dm=new ResourceDMBean(p);
        dm.forAllAttributes((k,v) -> {
            MBeanAttributeInfo info=v.info();
            String descr=info != null? info.getDescription() : "n/a";
            Supplier<Object> getter=() -> {
                try {
                    return v.getter() != null? v.getter().invoke(null) : null;
                }
                catch(Exception e) {
                    System.err.printf("failed getting value for %s\n", k);
                    return null;
                }
            };
            map.put(k, new Entry(descr, getter));
        });

        return map;
    }

    protected void start() throws Exception {
        ch=new JChannel().connect("bla").name("X");
        Map<String,Map<String,Entry>> map=extract(ch);
        for(Map.Entry<String,Map<String,Entry>> e: map.entrySet()) {
            System.out.printf("\n%s:\n---------------\n", e.getKey());
            for(Map.Entry<String,Entry> e2: e.getValue().entrySet()) {
                System.out.printf("  %s: %s\n", e2.getKey(), e2.getValue());
            }
        }
        Util.close(ch);
    }

    public static void main(String[] args) throws Throwable {
        new ExtractMetrics().start();

    }
}



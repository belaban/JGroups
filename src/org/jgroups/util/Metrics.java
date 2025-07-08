package org.jgroups.util;

import org.jgroups.JChannel;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.jmx.ResourceDMBean;
import org.jgroups.stack.Protocol;

import javax.management.MBeanAttributeInfo;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.jgroups.conf.AttributeType.BYTES;
import static org.jgroups.conf.AttributeType.SCALAR;

/**
 * Extracts all attributes and methods annotated with {@link org.jgroups.annotations.ManagedAttribute} and returns them
 * as a map of names associated with [getter-method/description tuples]. E.g. for an attribute called foo, a method
 * foo() or getFoo() is searched for.
 * @author Bela Ban
 * @since  5.4, 5.3.6
 */
public class Metrics {
    protected JChannel ch;
    public static final Predicate<AccessibleObject> IS_NUMBER=obj -> {
        if(obj instanceof Field)
            return isNumberAndScalar(obj, ((Field)obj).getType());
        if(obj instanceof Method) {
            Method m=(Method)obj;
            return isNumberAndScalar(obj, m.getReturnType());
        }
        return false;
    };
    public static final Predicate<AccessibleObject> IS_MANAGED_ATTRIBUTE=obj -> obj.getAnnotation(ManagedAttribute.class) != null;


    public static class Entry<T> {
        protected final AccessibleObject type; // Field or Method
        protected final String           description;
        protected final Supplier<T>      supplier;

        protected Entry(AccessibleObject type, String description, Supplier<T> method) {
            this.type=type;
            this.description=description;
            this.supplier=method;
        }

        public AccessibleObject type()        {return type;}
        public String           description() {return description;}
        public Supplier<T>      supplier()    {return supplier;}

        @Override
        public String toString() {
            return String.format("  %s [%s]", supplier.get(), description);
        }
    }

    public static Map<String,Map<String,Entry<Object>>> extract(JChannel ch) {
        return extract(ch, null);
    }

    public static Map<String,Map<String,Entry<Object>>> extract(JChannel ch, Predicate<AccessibleObject> filter) {
        Map<String,Map<String,Entry<Object>>> map=new LinkedHashMap<>();
        for(Protocol p: ch.stack().getProtocols())
            map.put(p.getName(), extract(p, filter));
        return map;
    }

    public static Map<String,Entry<Object>> extract(Protocol p) {
        return extract(p, null);
    }

    public static Map<String,Entry<Object>> extract(Protocol p, Predicate<AccessibleObject> filter) {
        Map<String,Entry<Object>> map=new TreeMap<>();
        ResourceDMBean dm=new ResourceDMBean(p, filter);
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
            map.put(k, new Entry<>(v.type(), descr, getter));
        });

        return map;
    }

    protected void start(boolean numeric) throws Exception {
        ch=new JChannel().connect("bla").name("X");
        Map<String,Map<String,Entry<Object>>> m=extract(ch, numeric? IS_NUMBER : null);
        if(numeric) {
            Map<String,Map<String,Entry<Number>>> map=convert(m);
            print(map);
        }
        else
            print(m);
        Util.close(ch);
    }

    protected static <T> void print(Map<String,Map<String,Entry<T>>> map) {
        for(Map.Entry<String,Map<String,Entry<T>>> e: map.entrySet()) {
            System.out.printf("\n%s:\n---------------\n", e.getKey());
            for(Map.Entry<String,Entry<T>> e2: e.getValue().entrySet()) {
                Entry<T> entry=e2.getValue();
                Supplier<T> s=entry.supplier();
                if(s != null)
                    System.out.printf("  %s: %s\n", e2.getKey(), s.get());
            }
        }
    }

    public static Map<String,Map<String,Entry<Number>>> convert(Map<String,Map<String,Entry<Object>>> m) {
        Map<String,Map<String,Entry<Number>>> retval=new LinkedHashMap<>();
        for(Map.Entry<String,Map<String,Entry<Object>>> entry: m.entrySet()) {
            Map<String,Entry<Object>> m1=entry.getValue();
            Map<String,Entry<Number>> m2=convertProtocol(m1);
            retval.put(entry.getKey(), m2);
        }
        return retval;
    }

    public static Map<String,Entry<Number>> convertProtocol(Map<String,Entry<Object>> m) {
        Map<String,Entry<Number>> retval=new TreeMap<>();
        for(Map.Entry<String,Entry<Object>> e: m.entrySet()) {
            Entry<Object> en=e.getValue();
            AccessibleObject type=en.type();
            Class<? extends Object> cl=(type instanceof Field)? ((Field)type).getType() : ((Method)type).getReturnType();
            if(Number.class.isAssignableFrom(cl)
                    || int.class.isAssignableFrom(cl) || long.class.isAssignableFrom(cl)
                    || float.class.isAssignableFrom(cl) || double.class.isAssignableFrom(cl)) {
                Entry<Number> tmp=new Entry<>(en.type(), en.description(), () -> (Number)en.supplier().get());
                retval.put(e.getKey(), tmp);
            }
            else if(boolean.class.isAssignableFrom(cl) || Boolean.class.isAssignableFrom(cl)) {
                Entry<Number> tmp=new Entry<>(en.type(), en.description(),
                        () -> Boolean.TRUE.equals((Boolean)en.supplier().get()) ? 1 : 0);
                retval.put(e.getKey(), tmp);
            }
            else if(AverageMinMax.class.isAssignableFrom(cl)) {
                Entry<Number> tmp=new Entry<>(en.type(), en.description(), 
                        () -> Optional.ofNullable((AverageMinMax)en.supplier().get()).map(AverageMinMax::min).orElse(null));
                retval.put(e.getKey() + ".min", tmp);
                tmp=new Entry<>(en.type(), en.description(),
                        () -> Optional.ofNullable((AverageMinMax)en.supplier().get()).map(AverageMinMax::average).orElse(null));
                retval.put(e.getKey(), tmp);
                tmp=new Entry<>(en.type(), en.description(),
                        () -> Optional.ofNullable((AverageMinMax)en.supplier().get()).map(AverageMinMax::max).orElse(null));
                retval.put(e.getKey() + ".max", tmp);
            }
            else if(Average.class.isAssignableFrom(cl)) {
                Entry<Number> tmp=new Entry<>(en.type(), en.description(),
                        () -> Optional.ofNullable((Average)en.supplier().get()).map(Average::average).orElse(null));
                retval.put(e.getKey(), tmp);
            }
        }
        return retval;
    }

    protected static boolean isNumberAndScalar(AccessibleObject obj, Class<?> cl) {
        if(cl.equals(float.class) || cl.equals(Float.class) || cl.equals(double.class) || cl.equals(Double.class)
          || Average.class.isAssignableFrom(cl))
            return true;
        boolean is_number=cl.equals(int.class) || cl.equals(Integer.class) || cl.equals(long.class) || cl.equals(Long.class)
          || Number.class.isAssignableFrom(cl);
        return is_number && isScalar(obj);
    }

    protected static boolean isScalar(AccessibleObject obj) {
        ManagedAttribute annotation=obj.getAnnotation(ManagedAttribute.class);
        if(annotation != null && (annotation.type() == SCALAR || annotation.type() == BYTES))
            return true;
        Property prop=obj.getAnnotation(Property.class);
        return prop != null && (prop.type() == SCALAR || prop.type() == BYTES);
    }

    public static void main(String[] args) throws Throwable {
        boolean numeric=args.length > 0 && Boolean.parseBoolean(args[0]);
        new Metrics().start(numeric);

    }
}



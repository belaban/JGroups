package org.jgroups.util;

import org.jgroups.annotations.Component;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

/**
 * Tools to (1) dump all protocols and the names of their attributes ({@link org.jgroups.annotations.ManagedAttribute})
 * and properties ({@link Property}) to file, (2) read from that file (old) and compare whether old is a proper
 * subset of new, ie. if all protocols and attributes/properties still have the same names in new.<br/>
 * To be run before releasing a new version (mainly minor and micro).
 * @author Bela Ban
 * @since 5.4.4
 */
public class CompareMetrics {
    protected static final String ROOT_PACKAGE="org.jgroups";
    protected static final String[] PACKAGES={
      "org.jgroups.protocols",
      "org.jgroups.protocols.pbcast",
      "org.jgroups.protocols.dns",
      "org.jgroups.protocols.relay"
    };

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String from_file=null, to_file=null;
        for(int i=0; i < args.length; i++) {
            if("-from".equals(args[i])) {
                from_file=args[++i];
                continue;
            }
            if("-to".equals(args[i])) {
                to_file=args[++i];
                continue;
            }
            System.out.printf("%s (-from <read from file> | -to <dump to file>)\n", CompareMetrics.class.getSimpleName());
            return;
        }

        // Sanity check
        if((from_file == null && to_file == null) || (from_file != null && to_file != null))
            throw new IllegalArgumentException("(only) one of '-from' or '-to' has to be defined");

        Map<String,Collection<String>> old_metrics=null;
        if(from_file != null) {
            old_metrics=readOldMetrics(from_file);
            long total_attrs=old_metrics.values().stream().mapToLong(Collection::size).sum();
            System.out.printf("-- read old metrics: %d protocols and %,d attributes\n", old_metrics.size(), total_attrs);
        }

        Map<String,Collection<String>> new_metrics=readCurrentMetrics();
        // System.out.printf("current metrics:\n%s\n", print(new_metrics));
        long total_attrs=new_metrics.values().stream().mapToLong(Collection::size).sum();
        System.out.printf("-- read current metrics: %d protocols and %,d attributes\n", new_metrics.size(), total_attrs);
        if(to_file != null) {
            // read the current metrics, dump to file and exit
            writeMetricsToFile(new_metrics, to_file);
            return;
        }

        // from_file was used to read the old metrics; compare with the current and then exit
        compareMetrics(new_metrics, old_metrics);

        old_metrics.put("UDP", List.of("number_of_messages"));
        new_metrics.put("UDP", List.of("num_msgs"));

        if(old_metrics.isEmpty() && new_metrics.isEmpty()) {
            System.out.println("\n** Success: both old and new metrics are the same");
            return;
        }
        if(!old_metrics.isEmpty()) {
            System.out.printf("\n** Failure: the following protocols/attributes are only found in old " +
                                "metrics, but not in new:\n%s\n", print(old_metrics));
        }
        if(!new_metrics.isEmpty()) {
            System.out.printf("\n** The following protocols/attributes are only found in new, but not in old " +
                                "(this may not be an error, e.g. when new protocols or attributes have been added):\n" +
                                "%s\n", print(new_metrics));
        }
    }

    protected static Map<String,Collection<String>> readOldMetrics(String from_file) throws IOException {
        Map<String,Collection<String>> map=new ConcurrentSkipListMap<>();
        try(InputStream in=new FileInputStream(from_file)) {
            for(;;) {
                String line=Util.readLine(in);
                if(line == null)
                    break;
                int index=line.indexOf(":");
                String protocol=line.substring(0, index).trim();
                index=line.indexOf("[");
                int end=line.indexOf("]");
                String attributes=line.substring(index + 1, end);
                StringTokenizer tok=new StringTokenizer(attributes, ",");
                Collection<String> attrs=new ConcurrentSkipListSet<>();
                while(tok.hasMoreTokens()) {
                    String attr=tok.nextToken().trim();
                    attrs.add(attr);
                }
                map.put(protocol, attrs);
            }
        }
        return map;
    }

    protected static Map<String,Collection<String>> readCurrentMetrics() throws IOException, ClassNotFoundException {
        SortedMap<String,Collection<String>> map=new ConcurrentSkipListMap<>();
        Set<Class<?>> classes=getProtocols();
        for(Class<?> cl: classes) {
            Collection<String> attrs=getAttributes(cl, null);
            if(!attrs.isEmpty())
                map.put(cl.getSimpleName(), attrs);
        }
        return map;
    }

    protected static Set<Class<?>> getProtocols() throws IOException, ClassNotFoundException {
        ClassLoader cl=Thread.currentThread().getContextClassLoader();
        Set<Class<?>> s=new HashSet<>();
        for(String p: PACKAGES) {
            Set<Class<?>> tmp=Util.findClassesAssignableFrom(p, Protocol.class, cl);
            s.addAll(tmp);
        }
        return s;
    }

    protected static void writeMetricsToFile(Map<String,Collection<String>> metrics, String to_file) throws IOException {
        try(OutputStream out=new FileOutputStream(to_file)) {
            for(Map.Entry<String,Collection<String>> e: metrics.entrySet()) {
                String s=String.format("%s: %s\n", e.getKey(), e.getValue());
                out.write(s.getBytes());
            }
        }
    }

    /**
     * Compares the new to the old metrics by removing 'old' from 'new' (if present in both 'new' and 'old').<br/>
     * If protocols/attributes remain, then either new protocols or attributes were added in new, or attributes /
     * protocols changed. E.g. an attribute changes from "number_of_messages" -> "num_msgs", then "number_of_attributes"
     * will remain in 'old' and "num_msgs" in 'new'.<br/>
     * The goal is that all attributes of 'old' also need to be in 'new', or else we have an incompatible change in that
     * an attribute was renamed or removed. If an attribute or protocol is only in 'new', that's acceptable and means
     * that it was added in 'new.
     */
    protected static void compareMetrics(Map<String,Collection<String>> new_metrics, Map<String,Collection<String>> old_metrics) {
        for(Iterator<Map.Entry<String,Collection<String>>> it=old_metrics.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String,Collection<String>> old_entry=it.next();
            String key=old_entry.getKey();
            Collection<String> old_attrs=old_entry.getValue();
            Collection<String> new_attrs=new_metrics.get(key);
            boolean rc=new_attrs != null && compareAttributes(new_attrs, old_attrs);
            if(rc) {
                it.remove();
                new_metrics.remove(key);
            }
        }
    }

    // Remove all attributes both in new and old
    // If both old and new are empty -> return true, else false
    protected static boolean compareAttributes(Collection<String> new_attrs, Collection<String> old_attrs) {
        for(Iterator<String> it=old_attrs.iterator(); it.hasNext();) {
            String old_attr=it.next();
            if(new_attrs.contains(old_attr)) {
                it.remove();
                new_attrs.remove(old_attr);
            }
        }
        return old_attrs.isEmpty() && new_attrs.isEmpty();
    }

    protected static String print(Map<String,Collection<String>> map) {
        return map.entrySet().stream().filter(e -> e.getValue() != null && !e.getValue().isEmpty())
          .map(e -> String.format("%s: %s", e.getKey(), e.getValue()))
          .collect(Collectors.joining("\n"));
    }

    protected static Collection<String> getAttributes(Class<?> clazz, String prefix)
      throws IOException, ClassNotFoundException {
        Collection<String> ret=new ConcurrentSkipListSet<>();
        Field[] fields=clazz.getDeclaredFields();
        for(Field field: fields) {
            if(field.isAnnotationPresent(Property.class)) {
                String property=field.getName();
                Property annotation=field.getAnnotation(Property.class);
                String name=annotation.name();
                if(name != null && !name.trim().isEmpty())
                    property=name.trim();
                if(prefix != null && !prefix.isEmpty())
                    property=prefix + "." + property;
                ret.add(property);
            }

            // is the field annotated with @Component?
            if(field.isAnnotationPresent(Component.class)) {
                Component ann=field.getAnnotation(Component.class);
                Class<?> type=field.getType();
                if(type.isInterface() || Modifier.isAbstract(type.getModifiers())) {
                    Set<Class<?>> implementations=Util.findClassesAssignableFrom(ROOT_PACKAGE, type, Thread.currentThread().getContextClassLoader());
                    for(Class<?> impl: implementations)
                        ret.addAll(getAttributes(impl, ann.name()));
                }
                else
                    ret.addAll(getAttributes(type, ann.name()));
            }
        }

        Method[] methods=clazz.getDeclaredMethods();
        for(Method method: methods) {
            if(method.isAnnotationPresent(Property.class)) {
                Property annotation=method.getAnnotation(Property.class);
                String name=annotation.name();
                if(name.isEmpty())
                    name=Util.methodNameToAttributeName(method.getName());
                if(prefix != null && !prefix.isEmpty())
                    name=prefix + "." + name;
                ret.add(name);
            }
        }
        return ret;
    }

    /**
     * Reads from the input stream and replaces occurrences of ${PROT} with p.get("PROT") and writes this to the
     * output stream. If no value is found, then the ${PROT} will simple be omitted from the output.
     * Escaped values of the form \${PROT} are not looked up and the value without the backslash will be written
     * to the output stream.
     */
    protected static void replaceVariables(InputStream in, OutputStream out, Properties p) {
        boolean looping=true;
        while(looping) {
            try {
                int ch=in.read(), n1, n2;
                if(ch == -1)
                    break;
                switch(ch) {
                    case '\\':
                        n1=in.read();
                        n2=in.read();
                        if(n1 == -1 || n2 == -1) {
                            looping=false;
                            if(n1 != -1)
                                out.write(n1);
                            break;
                        }

                        if(n1 == '$' && n2 == '{') {
                            String s=readUntilBracket(in);
                            out.write(n1);
                            out.write(n2);
                            out.write(s.getBytes());
                            out.write('}');
                        }
                        else {
                            out.write(ch);
                            out.write(n1);
                            out.write(n2);
                        }
                        break;
                    case '$':
                        n1=in.read();
                        if(n1 == -1) {
                            out.write(ch);
                            looping=false;
                        }
                        else {
                            if(n1 == '{') {
                                String s=readUntilBracket(in);
                                writeVarToStream(s, p, out);
                            }
                            else {
                                out.write(ch);
                                out.write(n1);
                            }
                        }
                        break;
                    default:
                        out.write(ch);
                }
            }
            catch(IOException e) {
                break;
            }
        }
        Util.close(in, out);
    }


    protected static void writeVarToStream(String var, Properties p, OutputStream out) throws IOException {
        String val=(String)p.get(var);
        if(val != null)
            out.write(val.getBytes());
    }

    /** Reads until the next bracket '}' and returns the string excluding the bracket, or throws an exception if
     * no bracket has been found */
    protected static String readUntilBracket(InputStream in) throws IOException {
        StringBuilder sb=new StringBuilder();
        while(true) {
            int ch=in.read();
            switch(ch) {
                case -1: throw new EOFException("no matching } found");
                case '}':
                    return sb.toString();
                default:
                    sb.append((char)ch);
            }
        }
    }

    public static int copy(Reader input, Writer output) throws IOException {
        char[] buffer = new char[8 * 1024];
        int count = 0;
        int n = 0;
        try {
            while (-1 != (n = input.read(buffer))) {
                output.write(buffer, 0, n);
                count += n;
            }
        } finally {
            output.flush();
            output.close();
        }
        return count;
    }
}

package org.jgroups.util;

import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Given a class, generates code for getters and setters for all attributes and writes it to stdout
 * @author Bela Ban
 * @since  5.0.0
 */
public class GenerateGettersAndSetters {

    public static void main(String[] args) throws ClassNotFoundException {
        String classname=null;
        boolean use_generics=false;

        for(int i=0; i < args.length; i++) {
            if("-class".equals(args[i])) {
                classname=args[++i];
                continue;
            }
            if("-use-generics".equals(args[i])) {
                use_generics=true;
                continue;
            }
            System.out.printf("%s -class <classname> [-use-generics>]\n", GenerateGettersAndSetters.class.getSimpleName());
            return;
        }

        if(classname == null) {
            System.err.println("no classname given (-class)");
            return;
        }

        Class<?> clazz=Util.loadClass(classname, GenerateGettersAndSetters.class);
        Field[] fields=clazz.getDeclaredFields();

        for(Field f: fields) {
            if(Modifier.isStatic(f.getModifiers()))
                continue; // skip static fields

            if(f.getAnnotation(ManagedAttribute.class) == null && f.getAnnotation(Property.class) == null)
                continue;

            Class<?> type=f.getType();
            String getter_name=Util.attributeNameToMethodName(f.getName()), setter_name=getter_name;
            boolean is_boolean=type.equals(boolean.class) || type.equals(Boolean.class);
            if(is_boolean) {
                getter_name=setter_name=getter_name.substring(0, 1).toLowerCase() + getter_name.substring(1);
            }
            else {
                getter_name="get" + getter_name;
                setter_name="set" + setter_name;
            }
            String typename=f.getType().getSimpleName();
            String var_name=f.getName().substring(0,1).toLowerCase();

            // System.out.printf("%s: %s %s\n", f.getName(), getter_name, setter_name);

            String getter=String.format("public %s %s() {return %s;}", typename, getter_name, f.getName());

            String retval=use_generics? String.format("<T extends %s> T", clazz.getSimpleName()) : clazz.getSimpleName();

            String setter=String.format("public %s %s(%s %s) {this.%s=%s; return %sthis;}",
                                        retval, setter_name, typename, var_name,
                                        f.getName(), var_name, use_generics? "(T)" : "");

            System.out.printf("%s\n%s\n\n", getter, setter);

        }

    }
}

package org.jgroups.blocks;


import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.*;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * A method call is the JGroups representation of a remote method.
 * It includes the name of the method (case sensitive) and a list of arguments.
 * A method call is serializable and can be passed over the wire.
 * @author Bela Ban
 */
public class MethodCall implements Externalizable, Streamable {

    private static final long    serialVersionUID=7873471327078957662L;

    /** The name of the method, case sensitive. */
    protected String             method_name;

    /** The ID of a method, maps to a java.lang.reflect.Method */
    protected short              method_id;

    /** The arguments of the method. */
    protected Object[]           args;

    /** The class types, e.g., new Class[]{String.class, int.class}. */
    protected Class[]            types;

    /** The Method of the call. */
    protected Method             method;

    protected static final Log   log=LogFactory.getLog(MethodCall.class);

    /** Which mode to use. */
    protected short              mode;

    protected MethodLookup       lookup;

    /** Explicitly ship the method, caller has to determine method himself. */
    public static final short    METHOD=1;

    /** Use class information. */
    public static final short    TYPES=2;

    /** Use an ID to map to a method */
    public static final short    ID=3;



    /**
     * Creates an empty method call, this is always invalid, until
     * <code>setName()</code> has been called.
     */
    public MethodCall() {
    }


    public MethodCall(Method method) {
        this(method, (Object[])null);
    }

    public MethodCall(Method method, Object... arguments) {
        init(method);
        if(arguments != null) args=arguments;
    }


    public MethodCall(short method_id, Object... args) {
        this.method_id=method_id;
        this.mode=ID;
        this.args=args;
    }


    public MethodCall(String method_name, Object[] args, Class[] types) {
        this.method_name=method_name;
        this.args=args;
        this.types=types;
        this.mode=TYPES;
    }


    private void init(Method method) {
        this.method=method;
        this.mode=METHOD;
        method_name=method.getName();
    }

    public MethodCall lookup(MethodLookup lookup) {this.lookup=lookup; return this;}

    public int getMode() {
        return mode;
    }


    /**
     * returns the name of the method to be invoked using this method call object
     * @return a case sensitive name, can be null for an invalid method call
     */
    public String getName() {
        return method_name;
    }

    /**
     * sets the name for this MethodCall and allowing you to reuse the same object for
     * a different method invokation of a different method
     * @param n - a case sensitive method name
     */
    public void setName(String n) {
        method_name=n;
    }

    public short getId() {
        return method_id;
    }

    public void setId(short method_id) {
        this.method_id=method_id;
    }

    /**
     * returns an ordered list of arguments used for the method invokation
     * @return returns the list of ordered arguments
     */
    public Object[] getArgs() {
        return args;
    }

    public void setArgs(Object...args) {
        this.args=args;
    }

    public Method getMethod() {
        return method;
    }


    public void setMethod(Method m) {
        init(m);
    }



    /** Called by the ProbeHandler impl. All args are strings. Needs to find a method where all parameter
     * types are primitive types, so the strings can be converted */
    public static Method findMethod(Class target_class, String method_name, Object[] args) throws Exception {
        int len=args != null? args.length : 0;
        Method retval=null;
        Method[] methods=getAllMethods(target_class);
        for(int i=0; i < methods.length; i++) {
            Method m=methods[i];
            if(m.getName().equals(method_name)) {
                Class<?>[] parameter_types=m.getParameterTypes();
                if(parameter_types.length == len) {
                    retval=m;
                    // now check if all parameter types are primitive types:
                    boolean all_primitive=true;
                    for(Class<?> parameter_type: parameter_types) {
                        if(!isPrimitiveType(parameter_type)) {
                            all_primitive=false;
                            break;
                        }
                    }
                    if(all_primitive)
                        return m;
                }
            }
        }
        return retval;
    }


    /**
     * The method walks up the class hierarchy and returns <i>all</i> methods of this class
     * and those inherited from superclasses and superinterfaces.
     */
    static Method[] getAllMethods(Class target) {
        Class superclass = target;
        List methods = new ArrayList();
        int size = 0;

        while(superclass != null) {
            try {
                Method[] m = superclass.getDeclaredMethods();
                methods.add(m);
                size += m.length;
                superclass = superclass.getSuperclass();
            }
            catch(SecurityException e) {
                // if it runs in an applet context, it won't be able to retrieve
                // methods from superclasses that belong to the java VM and it will
                // raise a security exception, so we catch it here.
                if(log.isWarnEnabled())
                    log.warn("unable to enumerate methods of superclass "+superclass+" of class "+target);
                superclass=null;
            }
        }

        Method[] result = new Method[size];
        int index = 0;
        for(Iterator i = methods.iterator(); i.hasNext();) {
            Method[] m = (Method[])i.next();
            System.arraycopy(m, 0, result, index, m.length);
            index += m.length;
        }
        return result;
    }

    /**
     * Returns the first method that matches the specified name and parameter types. The overriding
     * methods have priority. The method is chosen from all the methods of the current class and all
     * its superclasses and superinterfaces.
     *
     * @return the matching method or null if no mathching method has been found.
     */
    static Method getMethod(Class target, String methodName, Class[] types) {

        if (types == null) {
            types = new Class[0];
        }

        Method[] methods = getAllMethods(target);
        methods: for(int i = 0; i < methods.length; i++) {
            Method m = methods[i];
            if (!methodName.equals(m.getName())) {
                continue;
            }
            Class[] parameters = m.getParameterTypes();
            if (types.length != parameters.length) {
                continue;
            }
            for(int j = 0; j < types.length; j++) {
                if(!parameters[j].isAssignableFrom(types[j])) {
                // if (!types[j].equals(parameters[j])) {
                    continue methods;
                }
            }
            return m;
        }
        return null;
    }


    /**
     * Invokes the method with the supplied arguments against the target object.
     * If a method lookup is provided, it will be used. Otherwise, the default
     * method lookup will be used.
     *
     * @param target - the object that you want to invoke the method on
     * @return an object
     */
    public Object invoke(Object target) throws Exception {
        if(target == null)
            throw new IllegalArgumentException("target is null");

        Class cl=target.getClass();
        Method meth=null;

        switch(mode) {
            case METHOD:
                if(this.method != null)
                    meth=this.method;
                break;
            case TYPES:
                meth = getMethod(cl, method_name, types);
                break;
            case ID:
                meth=lookup != null? lookup.findMethod(method_id) : null;
                break;
            default:
                if(log.isErrorEnabled()) log.error("mode " + mode + " is invalid");
                break;
        }

        if(meth != null) {
            try {
                return meth.invoke(target, args);
            }
            catch(InvocationTargetException target_ex) {
                Throwable exception=target_ex.getTargetException();
                if(exception instanceof Error) throw (Error)exception;
                else if(exception instanceof RuntimeException) throw (RuntimeException)exception;
                else if(exception instanceof Exception) throw (Exception)exception;
                else throw new RuntimeException(exception);
            }
        }
        else {
            throw new NoSuchMethodException(method_name);
        }
    }

    public Object invoke(Object target, Object[] args) throws Exception {
        if(args != null)
            this.args=args;
        return invoke(target);
    }


    static Class[] getTypesFromString(Class cl, String[] signature) throws Exception {
        String  name;
        Class   parameter;
        Class[] mytypes=new Class[signature.length];

        for(int i=0; i < signature.length; i++) {
            name=signature[i];
            if("long".equals(name))
                parameter=long.class;
            else if("int".equals(name))
                parameter=int.class;
            else if("short".equals(name))
                parameter=short.class;
            else if("char".equals(name))
                parameter=char.class;
            else if("byte".equals(name))
                parameter=byte.class;
            else if("float".equals(name))
                parameter=float.class;
            else if("double".equals(name))
                parameter=double.class;
            else if("boolean".equals(name))
                parameter=boolean.class;
            else
                parameter=Class.forName(name, false, cl.getClassLoader());
            mytypes[i]=parameter;
        }
        return mytypes;
    }


    public String toString() {
        StringBuilder ret=new StringBuilder();
        boolean first=true;
        if(method_name != null)
            ret.append(method_name);
        else
            ret.append(method_id);
        ret.append('(');
        if(args != null) {
            for(int i=0; i < args.length; i++) {
                if(first)
                    first=false;
                else
                    ret.append(", ");
                ret.append(args[i]);
            }
        }
        ret.append(')');
        return ret.toString();
    }

    public String toStringDetails() {
        StringBuilder ret=new StringBuilder();
        ret.append("MethodCall ");
        if(method_name != null)
            ret.append("name=").append(method_name);
        else
            ret.append("id=").append(method_id);
        ret.append(", number of args=").append((args != null? args.length : 0)).append(')');
        if(args != null) {
            ret.append("\nArgs:");
            for(int i=0; i < args.length; i++) {
                ret.append("\n[").append(args[i]).append(" (").
                        append((args[i] != null? args[i].getClass().getName() : "null")).append(")]");
            }
        }
        return ret.toString();
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        if(method_name != null) {
            out.writeBoolean(true);
            out.writeUTF(method_name);
        }
        else {
            out.writeBoolean(false);
            out.writeShort(method_id);
        }
        out.writeObject(args);
        out.writeShort(mode);

        switch(mode) {
        case METHOD:
            out.writeObject(method.getParameterTypes());
            out.writeObject(method.getDeclaringClass());
            break;
        case TYPES:
            out.writeObject(types);
            break;
        case ID:
            break;
        default:
            if(log.isErrorEnabled()) log.error("mode " + mode + " is invalid");
            break;
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        boolean name_available=in.readBoolean();
        if(name_available)
            method_name=in.readUTF();
        else
            method_id=in.readShort();
        args=(Object[])in.readObject();
        mode=in.readShort();

        switch(mode) {
        case METHOD:
            Class[] parametertypes=(Class[])in.readObject();
            Class   declaringclass=(Class)in.readObject();
            try {
                method=declaringclass.getDeclaredMethod(method_name, parametertypes);
            }
            catch(NoSuchMethodException e) {
                throw new IOException(e.toString());
            }
            break;
        case TYPES:
            types=(Class[])in.readObject();
            break;
        case ID:
            break;
        default:
            if(log.isErrorEnabled()) log.error("mode " + mode + " is invalid");
            break;
        }
    }

    public void writeTo(DataOutput out) throws Exception {
        out.write(mode);

        switch(mode) {
            case METHOD:
                Bits.writeString(method_name,out);
                writeMethod(out);
                break;
            case TYPES:
                Bits.writeString(method_name,out);
                writeTypes(out);
                break;
            case ID:
                out.writeShort(method_id);
                break;
            default:
                throw new IllegalStateException("mode " + mode + " unknown");
        }
        writeArgs(out);
    }



    public void readFrom(DataInput in) throws Exception {
        mode=in.readByte();

        switch(mode) {
            case METHOD:
                method_name=Bits.readString(in);
                readMethod(in);
                break;
            case TYPES:
                method_name=Bits.readString(in);
                readTypes(in);
                break;
            case ID:
                method_id=in.readShort();
                break;
            default:
                throw new IllegalStateException("mode " + mode + " unknown");
        }
        readArgs(in);
    }


    protected void writeArgs(DataOutput out) throws Exception {
        int args_len=args != null? args.length : 0;
        out.write(args_len);
        if(args_len > 0) {
            for(Object obj: args)
                Util.objectToStream(obj, out);
        }
    }

    protected void readArgs(DataInput in) throws Exception {
        int args_len=in.readByte();
        if(args_len > 0) {
            args=new Object[args_len];
            for(int i=0; i < args_len; i++)
                args[i]=Util.objectFromStream(in);
        }
    }


    protected void writeTypes(DataOutput out) throws Exception {
        int types_len=types != null? types.length : 0;
        out.write(types_len);
        if(types_len > 0)
            for(Class<?> type: types)
                Util.objectToStream(type, out);
    }

    protected void readTypes(DataInput in) throws Exception {
        int types_len=in.readByte();
        if(types_len > 0) {
            types=new Class<?>[types_len];
            for(int i=0; i < types_len; i++)
                types[i]=(Class)Util.objectFromStream(in);
        }
    }

    protected void writeMethod(DataOutput out) throws Exception {
        if(method != null) {
            out.write(1);
            Util.objectToStream(method.getParameterTypes(),out);
            Util.objectToStream(method.getDeclaringClass(),out);
        }
        else
            out.write(0);
    }

    protected void readMethod(DataInput in) throws Exception {
        if(in.readByte() == 1) {
            Class[] parametertypes=(Class[])Util.objectFromStream(in);
            Class   declaringclass=(Class)Util.objectFromStream(in);
            try {
                method=declaringclass.getDeclaredMethod(method_name, parametertypes);
            }
            catch(NoSuchMethodException e) {
                throw new IOException(e.toString());
            }
        }
    }







    public static Object convert(String arg, Class<?> type) {
        if(type == String.class)
            return arg;
        if(type == boolean.class || type == Boolean.class)
            return Boolean.valueOf(arg);
        if(type == byte.class || type == Byte.class)
            return Byte.valueOf(arg);
        if(type == short.class || type == Short.class)
            return Short.valueOf(arg);
        if(type == int.class || type == Integer.class)
            return Integer.valueOf(arg);
        if(type == long.class || type == Long.class)
            return Long.valueOf(arg);
        if(type == float.class || type == Float.class)
            return Float.valueOf(arg);
        if(type == double.class || type == Double.class)
            return Double.valueOf(arg);
        return arg;
    }

    public static boolean isPrimitiveType(Class<?> type) {
        return type == String.class ||
          type == boolean.class     || type == Boolean.class   ||
          type == char.class        || type == Character.class ||
          type == byte.class        || type == Byte.class      ||
          type == short.class       || type == Short.class     ||
          type == int.class         || type == Integer.class   ||
          type == long.class        || type == Long.class      ||
          type == float.class       || type == Float.class     ||
          type == double.class      || type == Double.class;
    }
}




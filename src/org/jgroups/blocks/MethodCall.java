package org.jgroups.blocks;


import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Vector;

import org.jgroups.log.Trace;




/**
 * A method call is the JavaGroup representation of a remote method.
 * It includes the name of the method (case sensitive) and a list of arguments.
 * A method call is serializable and can be passed over the wire.
 * @author Bela Ban
 * @version $Revision: 1.3 $
 */
public class MethodCall implements Externalizable {

    static final long serialVersionUID=7873471327078957662L;

    /** the name of the method, case sensitive */
    protected String method_name=null;

    /** the arguments of the method */
    protected Object[] args=new Object[0];

    /** the class types, e.g. new Class[]{String.class, int.class} */
    protected Class[] types=null;

    /** the signature, e.g. new String[]{String.class.getName(), int.class.getName()} */
    protected String[] signature=null;

    /** the Method of the call */
    protected Method method=null;

    /** which mode to use */
    protected short mode=OLD;

    /** infer the method from the arguments */
    protected static final short OLD=1;

    /** explicitly ship the method, caller has to determine method himself */
    protected static final short METHOD=2;

    /** use class information */
    protected static final short TYPES=3;

    /** provide a signature, similar to JMX */
    protected static final short SIGNATURE=4;


    /**
     * creates an empty method call, this is always invalid, until
     * <code>SetName</code> has been called
     */
    public MethodCall() {
    }

    public MethodCall(Method method) {
        this(method, null);
    }

    public MethodCall(Method method, Object[] arguments) {
        init(method);
        if(arguments != null) args=arguments;
    }


    public void init(Method method) {
        this.method=method;
        this.mode=METHOD;
        method_name=method.getName();
    }

    /**
     * creates a method call without arguments for a certain method.
     * Arguments can always be added using the AddArg method
     * @param name the name of the method, cannot be null
     * @deprecated use the complete constructor with a java.lang.reflect.Method argument since null
     * arguments can not be correctly handled using null arguments
     * @see #MethodCall(java.lang.reflect.Method)
     */
    public MethodCall(String name) {
        method_name=name;
    }

    /**
     * creates a MethodCall with the given name and the given arguments
     * @param name the name of the method, cannot be null
     * @param args an array of the arguments, can be null (no arguments)
     * @deprecated use the complete constructor with a java.lang.reflect.Method argument since null
     * arguments can not be correctly handled using null arguments
     * @see #MethodCall(java.lang.reflect.Method)
     */
    public MethodCall(String name, Object[] args) {
        this(name);
        if(args != null) {
            for(int i=0; i < args.length; i++) {
                addArg(args[i]);
            }
        }
    }

    /**
     * creates a MethodCall with the given name and the given arguments
     * @param name the name of the method, cannot be null
     * @param arg1 an object
     * @deprecated use the complete constructor with a java.lang.reflect.Method argument since null
     * arguments can not be correctly handled using null arguments
     * @see #MethodCall(java.lang.reflect.Method)
     */
    public MethodCall(String name, Object arg1) {
        this(name, new Object[]{arg1});
    }


    /**
     * creates a MethodCall with the given name and the given arguments
     * @param name the name of the method, cannot be null
     * @param arg1 first argument
     * @param arg1 second argument
     * @deprecated use the complete constructor with a java.lang.reflect.Method argument since null
     * arguments can not be correctly handled using null arguments
     * @see #MethodCall(java.lang.reflect.Method)
     */
    public MethodCall(String name, Object arg1, Object arg2) {
        this(name, new Object[]{arg1, arg2});
    }


    /**
     * creates a MethodCall with the given name and the given arguments
     * @param name the name of the method, cannot be null
     * @param arg1 first argument
     * @param arg2 second argument
     * @param arg3 third argument
     * @deprecated use the complete constructor with a java.lang.reflect.Method argument since null
     * arguments can not be correctly handled using null arguments
     * @see #MethodCall(java.lang.reflect.Method)
     */
    public MethodCall(String name, Object arg1, Object arg2, Object arg3) {
        this(name, new Object[]{arg1, arg2, arg3});
    }


    /**
     * creates a MethodCall with the given name and the given arguments
     * @param name the name of the method, cannot be null
     * @param arg1 first argument
     * @param arg2 second argument
     * @param arg3 third argument
     * @param arg4 fourth argument
     * @deprecated use the complete constructor with a java.lang.reflect.Method argument since null
     * arguments can not be correctly handled using null arguments
     * @see #MethodCall(java.lang.reflect.Method)
     */
    public MethodCall(String name, Object arg1, Object arg2, Object arg3, Object arg4) {
        this(name, new Object[]{arg1, arg2, arg3, arg4});
    }


    /**
     * creates a MethodCall with the given name and the given arguments
     * @param name the name of the method, cannot be null
     * @param arg1 first argument
     * @param arg2 second argument
     * @param arg3 third argument
     * @param arg4 fourth argument
     * @param arg5 firth argument
     * @deprecated use the complete constructor with a java.lang.reflect.Method argument since null
     * arguments can not be correctly handled using null arguments
     * @see #MethodCall(java.lang.reflect.Method)
     */
    public MethodCall(String name, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
        this(name, new Object[]{arg1, arg2, arg3, arg4, arg5});
    }


    public MethodCall(String method_name, Object[] args, Class[] types) {
        this.method_name=method_name;
        this.args=args;
        this.types=types;
        this.mode=TYPES;
    }

    public MethodCall(String method_name, Object[] args, String[] signature) {
        this.method_name=method_name;
        this.args=args;
        this.signature=signature;
        this.mode=SIGNATURE;
    }


    public int getMode() {
        return mode;
    }

    /**
     * Allocates an array for a given number of arguments. Using setArgs() will help us avoid argument array
     * allocation/deallocation if the number of args is constant
     */
    public void setNumArgs(int num) {
        args=new Object[num];
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

    /**
     * returns an ordered list of arguments used for the method invokation
     * @return returns the list of ordered arguments
     */
    public Vector getArgs() {
        Vector v=new Vector();
        for(int i=0; i < args.length; i++)
            v.addElement(args[i]);
        return v;
    }

    public Method getMethod() {
        return method;
    }


    /**
     * adds an argument to the end of the argument list
     * @param arg - object argument for the method invokation
     */
    public void addArg(Object arg) {
        Object[] newarg=new Object[args.length + 1];
        System.arraycopy(args, 0, newarg, 0, args.length);
        newarg[args.length]=arg;
        args=newarg;
    }

    /**
     * adds a primitive byte as an argument to the end of the argument list
     * @param b - a byte argument for the method invokation
     */
    public void addArg(byte b) {
        Byte obj=new Byte(b);
        addArg(obj);
    }

    /**
     * adds a primitive char as an argument to the end of the argument list
     * @param c - a char argument for the method invokation
     */
    public void addArg(char c) {
        Character obj=new Character(c);
        addArg(obj);
    }

    /**
     * adds a primitive boolean as an argument to the end of the argument list
     * @param b - a boolean argument for the method invokation
     */
    public void addArg(boolean b) {
        Boolean obj=new Boolean(b);
        addArg(obj);
    }

    /**
     * adds a primitive int as an argument to the end of the argument list
     * @param i - an int argument for the method invokation
     */
    public void addArg(int i) {
        Integer obj=new Integer(i);
        addArg(obj);
    }

    /**
     * adds a primitive long as an argument to the end of the argument list
     * @param l - a long argument for the method invokation
     */
    public void addArg(long l) {
        Long obj=new Long(l);
        addArg(obj);
    }


    /**
     * Sets the argument at index to arg. If index is smaller than args.length(), addArg() will be used instead
     */
    public void setArg(int index, Object arg) {
        if(index >= args.length)
            addArg(arg);
        else
            args[index]=arg;
    }


    public void clearArgs() {
        args=new Object[0];
    }


    /**
     * returns a method instance for a certain class matching the name
     * and the argument types
     * @param target_class - the class that you will invoke the method on
     * @param method_name - the name of the method
     * @param args - the list of arguments (or just class instances of the arguments. args[i].getClass() will be used
     * in this method, not the actual value.
     */
    Method findMethod(Class target_class, String method_name, Vector args) throws Exception {
        int     len=args != null? args.size() : 0;
        Class[] formal_parms=new Class[len];
        Method  retval;

        for(int i=0; i < len; i++) {
            formal_parms[i]=args.elementAt(i).getClass();
        }

        /* getDeclaredMethod() is a bit faster, but only searches for methods in the current
        class, not in superclasses */
        retval=target_class.getMethod(method_name, formal_parms);

        return retval;
    }



    /**
     * Invokes the method with the supplied arguments against the target object.
     * If a method lookup is provided, it will be used. Otherwise, the default
     * method lookup will be used.
     * @param target - the object that you want to invoke the method on
     * @param lookup - an object that allows you to lookup the method on a target object
     *                 if null is passed as a parameter, the internal FindMethod will be used
     *                 using core reflection
     *
     * @return an object
     */
    public Object invoke(Object target, MethodLookup lookup) {
        Class  cl;
        Method meth=null;
        Object retval=null;


        if(method_name == null || target == null) {
            Trace.error("MethodCall.invoke()", "method name or target is null");
            return null;
        }
        cl=target.getClass();
        try {
            switch(mode) {
                case OLD:
                    if(lookup == null)
                        meth=findMethod(cl, method_name, getArgs());
                    else
                        meth=lookup.findMethod(cl, method_name, getArgs());
                    break;
                case METHOD:
                    if(this.method != null)
                        meth=this.method;
                    break;
                case TYPES:
                    meth=cl.getMethod(method_name, types);
                    break;
                case SIGNATURE:
                    Class[] mytypes=null;
                    if(signature != null)
                        mytypes=getTypesFromString(cl, signature);
                    meth=cl.getMethod(method_name, mytypes);
                    break;
                default:
                    Trace.error("MethodCall.invoke()", "mode " + mode + " is invalid");
                    break;
            }

            if(meth != null) {
                retval=meth.invoke(target, args);
            }
            else {
                Trace.error("MethodCall.invoke()", "method " + method_name + " not found");
            }
            return retval;
        }
        catch(IllegalArgumentException illegal_arg) {
            return illegal_arg;
        }
        catch(IllegalAccessException illegal_access) {
            illegal_access.printStackTrace();
            return illegal_access;
        }
        catch(InvocationTargetException inv_ex) {
            return inv_ex.getTargetException();
        }
        catch(NoSuchMethodException no) {
            System.out.print("MethodCall.invoke(): found no method called " + method_name +
                             " in class " + cl.getName() + " with [");
            for(int i=0; i < args.length; i++) {
                if(i > 0)
                    System.out.print(", ");
                System.out.print((args[i] != null)? args[i].getClass().getName() : "null");
            }
            System.out.println("] formal parameters");
            return no;
        }
        catch(Throwable e) {
            Trace.error("MethodCall.invoke()", "exception=" + e);
            return e;
        }
    }

    Class[] getTypesFromString(Class cl, String[] signature) throws Exception {
        String  name;
        Class   parameter;
        Class[] mytypes=new Class[signature.length];

        for(int i=0; i < signature.length; i++) {
            name=signature[i];
            if(name.equals("long"))
                parameter=long.class;
            else if(name.equals("int"))
                parameter=int.class;
            else if(name.equals("short"))
                parameter=short.class;
            else if(name.equals("char"))
                parameter=char.class;
            else if(name.equals("byte"))
                parameter=byte.class;
            else if(name.equals("float"))
                parameter=float.class;
            else if(name.equals("double"))
                parameter=double.class;
            else if(name.equals("boolean"))
                parameter=boolean.class;
            else
                parameter=Class.forName(name, false, cl.getClassLoader());
            mytypes[i]=parameter;
        }
        return mytypes;
    }


    public String toString() {
        StringBuffer ret=new StringBuffer();
        boolean first=true;
        ret.append(method_name).append("(");
        for(int i=0; i < args.length; i++) {
            if(first) {
                first=false;
            }
            else {
                ret.append(", ");
            }
            ret.append(args[i]);
        }
        ret.append(")");
        return ret.toString();
    }

    public String toStringDetails() {
         StringBuffer ret=new StringBuffer();
         ret.append("MethodCall (name=" + method_name);
         ret.append(", number of args=" + args.length + ")");
         ret.append("\nArgs:");
         for(int i=0; i < args.length; i++) {
             ret.append("\n[" + args[i] + " (" +
                        (args[i] != null? args[i].getClass().getName() : "null") + ")]");
         }
         return ret.toString();
     }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(method_name);
        out.writeObject(args);
        out.writeShort(mode);

        switch(mode) {
            case OLD:
                break;
            case METHOD:
                out.writeObject(method.getParameterTypes());
                out.writeObject(method.getDeclaringClass());
                break;
            case TYPES:
                out.writeObject(types);
                break;
            case SIGNATURE:
                out.writeObject(signature);
                break;
            default:
                Trace.error("MethodCall.readExternal()", "mode " + mode + " is invalid");
                break;
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        method_name=in.readUTF();
        args=(Object[])in.readObject();
        mode=in.readShort();

        switch(mode) {
            case OLD:
                break;
            case METHOD:
                Class[] parametertypes=(Class[])in.readObject();
                Class   declaringclass=(Class)in.readObject();
                try {
                    method=declaringclass.getMethod(method_name, parametertypes);
                }
                catch(NoSuchMethodException e) {
                    throw new IOException(e.toString());
                }
                break;
            case TYPES:
                types=(Class[])in.readObject();
                break;
            case SIGNATURE:
                signature=(String[])in.readObject();
                break;
            default:
                Trace.error("MethodCall.readExternal()", "mode " + mode + " is invalid");
                break;
        }
    }


    public static void main(String[] args) throws Exception {
        MethodCall m=new MethodCall(MethodCall.class.getMethod("invoke", new Class[]{Object.class}));
        m.addArg(m);
        ByteArrayOutputStream msg_data=new ByteArrayOutputStream();
        ObjectOutputStream msg_out=new ObjectOutputStream(msg_data);
        m.writeExternal(msg_out);
        msg_out.flush();
        msg_out.close();
        byte[] data=msg_data.toByteArray();
        ByteArrayInputStream msg_in_data=new ByteArrayInputStream(data);
        ObjectInputStream msg_in=new ObjectInputStream(msg_in_data);
        MethodCall m2=new MethodCall();
        m2.readExternal(msg_in);
        System.out.println(m2.getName());
        System.out.println(m2.getArgs().size());

    }

}




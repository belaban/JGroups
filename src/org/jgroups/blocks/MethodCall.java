package org.jgroups.blocks;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.util.Util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;




/**
 * A method call is the JavaGroup representation of a remote method.
 * It includes the name of the method (case sensitive) and a list of arguments.
 * A method call is serializable and can be passed over the wire.
 * @author Bela Ban
 * @version $Revision: 1.12 $
 */
public class MethodCall implements Externalizable {

    static final long serialVersionUID=7873471327078957662L;

    /** the name of the method, case sensitive */
    protected String method_name=null;

    /** the arguments of the method */
    protected Object[] args=null;

    /** the class types, e.g. new Class[]{String.class, int.class} */
    protected Class[] types=null;

    /** the signature, e.g. new String[]{String.class.getName(), int.class.getName()} */
    protected String[] signature=null;

    /** the Method of the call */
    protected Method method=null;

    protected static Log log=LogFactory.getLog(MethodCall.class);

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
     * <code>setName()</code> has been called
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

    /**
     *
     * @param method_name
     * @param args
     * @deprecated Use one of the constructors that take class types as arguments
     */
    public MethodCall(String method_name, Object[] args) {
        this.method_name=method_name;
        this.mode=OLD;
        this.args=args;
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

    void init(Method method) {
        this.method=method;
        this.mode=METHOD;
        method_name=method.getName();
    }


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

    /**
     * returns an ordered list of arguments used for the method invokation
     * @return returns the list of ordered arguments
     */
    public Object[] getArgs() {
        return args;
    }

    public void setArgs(Object[] args) {
        if(args != null)
            this.args=args;
    }

    public Method getMethod() {
        return method;
    }




    /**
     *
     * @param target_class
     * @return
     * @throws Exception
     */
    Method findMethod(Class target_class) throws Exception {
        int     len=args != null? args.length : 0;
        Method  m;

        Method[] methods=target_class.getMethods();
        for(int i=0; i < methods.length; i++) {
            m=methods[i];
            if(m.getName().equals(method_name)) {
                if(m.getParameterTypes().length == len)
                    return m;
            }
        }

        return null;
    }

//    Method findMethod(Class target_class) throws Exception {
//        int     len=args != null? args.length : 0;
//        Class[] formal_parms=new Class[len];
//        Method  retval;
//
//        for(int i=0; i < len; i++) {
//            formal_parms[i]=args[i].getClass();
//        }
//
//        /* getDeclaredMethod() is a bit faster, but only searches for methods in the current
//        class, not in superclasses */
//        retval=target_class.getMethod(method_name, formal_parms);
//        return retval;
//    }



    /**
     * Invokes the method with the supplied arguments against the target object.
     * If a method lookup is provided, it will be used. Otherwise, the default
     * method lookup will be used.
     * @param target - the object that you want to invoke the method on
     * @return an object
     */
    public Object invoke(Object target) throws Throwable {
        Class  cl;
        Method meth=null;
        Object retval=null;


        if(method_name == null || target == null) {
            if(log.isErrorEnabled()) log.error("method name or target is null");
            return null;
        }
        cl=target.getClass();
        try {
            switch(mode) {
                case OLD:
                    meth=findMethod(cl);
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
                    if(log.isErrorEnabled()) log.error("mode " + mode + " is invalid");
                    break;
            }

            if(meth != null) {
                retval=meth.invoke(target, args);
            }
            else {
                if(log.isErrorEnabled()) log.error("method " + method_name + " not found");
            }
            return retval;
        }
        catch(InvocationTargetException inv_ex) {
            throw inv_ex.getTargetException();
        }
        catch(NoSuchMethodException no) {
            StringBuffer sb=new StringBuffer();
            sb.append("found no method called ").append(method_name).append(" in class ");
            sb.append(cl.getName()).append(" with (");
            if(args != null) {
                for(int i=0; i < args.length; i++) {
                    if(i > 0)
                        sb.append(", ");
                    sb.append((args[i] != null)? args[i].getClass().getName() : "null");
                }
            }
            sb.append(") formal parameters");
            log.error(sb.toString());
            throw no;
        }
        catch(Throwable e) {
            // e.printStackTrace(System.err);
            if(log.isErrorEnabled()) log.error("exception=" + Util.printStackTrace(e));
            throw e;
        }
    }

    public Object invoke(Object target, Object[] args) throws Throwable {
        if(args != null)
            this.args=args;
        return invoke(target);
    }


    Class[] getTypesFromString(Class cl, String[] signature) throws Exception {
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
        StringBuffer ret=new StringBuffer();
        boolean first=true;
        ret.append(method_name).append('(');
        if(args != null) {
            for(int i=0; i < args.length; i++) {
                if(first) {
                    first=false;
                }
                else {
                    ret.append(", ");
                }
                ret.append(args[i]);
            }
        }
        ret.append(')');
        return ret.toString();
    }

    public String toStringDetails() {
        StringBuffer ret=new StringBuffer();
        ret.append("MethodCall (name=" + method_name);
        ret.append(", number of args=" + (args != null? args.length : 0) + ')');
        if(args != null) {
            ret.append("\nArgs:");
            for(int i=0; i < args.length; i++) {
                ret.append("\n[" + args[i] + " (" +
                           (args[i] != null? args[i].getClass().getName() : "null") + ")]");
            }
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
//                out.writeInt(signature != null? signature.length : 0);
//                for(int i=0; i < signature.length; i++) {
//                    String s=signature[i];
//                    out.writeUTF(s);
//                }
                break;
            default:
                if(log.isErrorEnabled()) log.error("mode " + mode + " is invalid");
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
//                int len=in.readInt();
//                if(len > 0) {
//                    signature=new String[len];
//                    for(int i=0; i < len; i++) {
//                        signature[i]=in.readUTF();
//                    }
//                }
                break;
            default:
                if(log.isErrorEnabled()) log.error("mode " + mode + " is invalid");
                break;
        }
    }




}




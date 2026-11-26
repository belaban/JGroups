package org.jgroups.blocks;


import org.jgroups.Constructable;
import org.jgroups.util.Bits;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Objects;
import java.util.function.Supplier;


/**
 * A method call is the JGroups representation of a remote method.
 * It includes the name of the method (case sensitive) and a list of arguments.
 * A method call is serializable and can be passed over the wire.
 * @author Bela Ban
 */
public class MethodCall implements Streamable, Constructable<MethodCall> {
    protected String       method_name;
    protected short        method_id=-1; // the ID of a method, maps to a java.lang.reflect.Method
    protected Object[]     args;         // the arguments to the call
    protected Class<?>[]   types;        // the types of the arguments, e.g., new Class[]{String.class, int.class}
    protected Method       method;


    /** Needed for deserialization */
    public MethodCall() {
    }


    public MethodCall(Method m, Object... arguments) {
        setMethod(m);
        if(arguments != null)
            args=arguments;
    }


    public MethodCall(short method_id, Object... args) {
        this.method_id=assertNotNegative(method_id);
        this.args=args;
    }


    public MethodCall(String method_name, Object[] args, Class<?>[] types) {
        this.method_name=method_name;
        this.args=args;
        this.types=types;
    }

    public Supplier<? extends MethodCall> create() {
        return MethodCall::new;
    }

    public boolean    useIds()                 {return method_id >= 0;}
    public String     getMethodName()          {return method_name != null? method_name : String.valueOf(method_id);}

    public short      getMethodId()            {return method_id;}
    public MethodCall setMethodId(short id)    {this.method_id=id; return this;}

    public Object[]   getArgs()                {return args;}
    public MethodCall setArgs(Object...args)   {this.args=args; return this;}

    public Method     getMethod()              {return method;}


    public MethodCall setMethod(Method m)      {
        this.method=Objects.requireNonNull(m);
        this.method_name=m.getName();
        this.types=m.getParameterTypes();
        return this;
    }



    /**
     * Invokes the method with the supplied arguments against the target object.
     * @param target - the object that you want to invoke the method on
     * @return the result
     */
    public Object invoke(Object target) throws Exception {
        if(target == null)
            throw new IllegalArgumentException("target is null");

        Method meth=this.method;
        if(!useIds())
            meth=this.method != null? this.method : Util.findMethod(target.getClass(), method_name, types); // target.getClass().getMethod(method_name, types);

        if(meth != null) {
            try {
                // allow method invocation on protected or (package-) private methods, too
                if(!Modifier.isPublic(meth.getModifiers()))
                    meth.setAccessible(true);
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
        else
            throw new NoSuchMethodException(method_name);
    }

    public Object invoke(Object target, Object[] args) throws Exception {
        if(args != null)
            this.args=args;
        return invoke(target);
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

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeBoolean(useIds());
        if(useIds())
            out.writeShort(method_id);
        else {
            Bits.writeString(method_name,out);
            writeTypes(out);
        }
        writeArgs(out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        if(in.readBoolean())
            method_id=in.readShort();
        else {
            method_name=Bits.readString(in);
            readTypes(in);
        }
        readArgs(in);
    }


    protected void writeArgs(DataOutput out) throws IOException {
        int args_len=args != null? args.length : 0;
        out.writeByte(args_len);
        if(args_len == 0)
            return;
        for(Object obj: args)
            writeArg(out, obj);
    }

    protected void writeArg(DataOutput out, Object obj) throws IOException {
        Util.objectToStream(obj, out);
    }

    protected void readArgs(DataInput in) throws IOException, ClassNotFoundException {
        int args_len=in.readByte();
        if(args_len == 0)
            return;
        args=new Object[args_len];
        for(int i=0; i < args_len; i++)
            args[i]=readArg(in);
    }

    protected Object readArg(DataInput in) throws IOException, ClassNotFoundException {
        return Util.objectFromStream(in);
    }

    protected void writeTypes(DataOutput out) throws IOException {
        int types_len=types != null? types.length : 0;
        out.writeByte(types_len);
        if(types_len > 0)
            for(Class<?> type: types)
                Util.objectToStream(type, out);
    }

    protected void readTypes(DataInput in) throws IOException, ClassNotFoundException {
        int types_len=in.readByte();
        if(types_len > 0) {
            types=new Class<?>[types_len];
            for(int i=0; i < types_len; i++)
                types[i]=Util.objectFromStream(in);
        }
    }

    protected void writeMethod(DataOutput out) throws IOException {
        if(method != null) {
            out.writeByte(1);
            Util.objectToStream(method.getParameterTypes(),out);
            Util.objectToStream(method.getDeclaringClass(),out);
        }
        else
            out.writeByte(0);
    }

    protected static short assertNotNegative(short num) {
        if(num < 0)
            throw new IllegalArgumentException(String.format("value (%d) has to be positive", num));
        return num;
    }



}




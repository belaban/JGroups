package org.jgroups.blocks.executor;

import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.concurrent.Callable;

public final class Executions {
	
	private Executions() {
		throw new InstantiationError( "Must not instantiate this class" );
	}

    /**
     * This method should be used to convert a callable that would not normally
     * be serializable, externalizable or streamable but has serializable,
     * externalizable or streamable arguments to a constructor to construct it.
     * <p>
     * When the call method is called on the callable it will call the provided
     * constructor passing in the given arguments. It will then invoke the call
     * method on resulting callable that was created.
     * <p>
     * The amount of arguments cannot exceed {@link Byte#MAX_VALUE}. Also the
     * constructor cannot exceed {@link Byte#MAX_VALUE} position in the
     * constructor array returned from {@link Class#getConstructors()}
     * <p>
     * The amount of arguments must match the amount of arguments required
     * by the constructor.  Also the arguments must be compatibile with the
     * types required of the constructor.
     * <p>
     * Unfortunately it isn't easy to pass a Constructor<? extends Callable<T>>
     * so we can't pass back a callable that is properly typed.  Also this
     * forces the caller to cast their callable or returned value to the correct 
     * type manually.
     * 
     * @param constructorToUse The constructor to use when creating the callable
     * @param args The arguments to pass to the constructor
     * @return The callable that will upon being called will instantiate the
     *         given callable using the constructor with the provided arguments
     *         and calls the call method
     * @throws IllegalArgumentException This is thrown if the arguments are
     *         not serializable, externalizable or streamable.  It can be thrown
     *         if the constructo is not accessible.  It can also be thrown
     *         if too many arguments or the constructor is to high up in the
     *         constructo array returned by the class.
     */
    public static Callable<?> serializableCallable(@SuppressWarnings("rawtypes") 
                Constructor<? extends Callable> constructorToUse, Object... args)
            throws IllegalArgumentException {
        if (args.length > (int)Byte.MAX_VALUE) {
            throw new IllegalArgumentException(
                "Max number of arguments exceeded: " + Byte.MAX_VALUE);
        }
        Class<?>[] params = constructorToUse.getParameterTypes();
        
        if (params.length != args.length) {
            throw new IllegalArgumentException("Number of arguments ["
                    + args.length + "] doesn't match number of arguments for "
                    + "constructor [" + params.length + "]");
        }
        
        for (int i = 0; i < args.length; ++i) {
            Object arg = args[i];
            if (arg instanceof Serializable ||
                    arg instanceof Streamable) {
                Class<?> classArg = params[i];
                
                if (!classArg.isInstance(arg)) {
                    throw new IllegalArgumentException("Argument [" + arg + 
                        "] is not an instance of [" + classArg + "]");
                }
            }
            else {
                throw new IllegalArgumentException(
                    "Argument is not serializable, externalizable or streamable: " + arg);
            }
        }
        @SuppressWarnings("unchecked")
        Class<? extends Callable<?>> classToUse = 
            (Class<? extends Callable<?>>)constructorToUse.getDeclaringClass();
        Constructor<?>[] constructors = classToUse.getConstructors();
        byte constructorPosition = -1;
        for (int i = 0; i < constructors.length; ++i) {
            Constructor<?> constructor = constructors[i];
            if (constructor.equals(constructorToUse)) {
                if (i > (int)Byte.MAX_VALUE) {
                    throw new IllegalArgumentException(
                        "Constructor position in array cannot be higher than "
                                + Byte.MAX_VALUE);
                }
                constructorPosition = (byte)i;
            }
        }
        if (constructorPosition == -1) {
            throw new IllegalArgumentException(
                "Constructor was not found in public constructor array on class");
        }
        return new StreamableCallable(classToUse, constructorPosition, args);
    }
    
    protected static class StreamableCallable implements Callable<Object>, Streamable {

        protected Class<? extends Callable<?>> _classCallable;
        protected short _constructorNumber;
        protected Object[] _args;
        
        public StreamableCallable() {
            
        }
        
        public StreamableCallable(Class<? extends Callable<?>> classCallable, 
                                    byte constructorNumber, Object... args) {
            _classCallable = classCallable;
            _constructorNumber = constructorNumber;
            _args = args;
        }

        @Override
        public Object call() throws Exception {
            @SuppressWarnings("unchecked")
            // Unfortunately getConstructors doesn't return typed constructors
            // correctly so we have to cast
            Constructor<? extends Callable<?>> constructor = 
                (Constructor<? extends Callable<?>>) _classCallable
                .getConstructors()[_constructorNumber];
            Callable<?> callable = constructor.newInstance(_args);
            return callable.call();
        }

        @Override
        public void writeTo(DataOutput out) throws Exception {
            Util.writeClass(_classCallable, out);
            out.writeByte(_constructorNumber);
            out.writeByte(_args.length);
            for (Object arg : _args) {
                try {
                    Util.writeObject(arg, out);
                }
                catch (Exception e) {
                    throw new IOException("failed to write arg " + arg);
                }
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void readFrom(DataInput in) throws Exception {
            try {
                _classCallable = (Class<? extends Callable<?>>)Util.readClass(in);
            }
            catch (ClassNotFoundException e) {
                throw new IOException("failed to read class from classname", e);
            }
            _constructorNumber = in.readByte();
            short numberOfArgs = in.readByte();
            _args = new Object[numberOfArgs];
            for (int i = 0; i < numberOfArgs; ++i) {
                try {
                    _args[i] = Util.readObject(in);
                }
                catch (Exception e) {
                    throw new IOException("failed to read arg", e);
                }
            }
        }

        // @see java.lang.Object#toString()
        @Override
        public String toString() {
            return "StreamableCallable [class=" + _classCallable
                    + ", constructor=" + _constructorNumber + ", arguments="
                    + Arrays.toString(_args) + "]";
        }
    }
}

package org.jgroups;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Factory to create messages. Uses an array for message IDs less then 32, and a hashmap for
 * types above 32
 * @author Bela Ban
 * @since  5.0
 */
public class MessageFactory {
    protected static final byte                            MIN_TYPE=32;
    protected final Supplier<? extends Message>[]          creators=new Supplier[MIN_TYPE];
    protected final Map<Short,Supplier<? extends Message>> map=new HashMap<>();

    private static volatile MessageFactory singleton=null;

    public synchronized static MessageFactory get() {
        MessageFactory mf=singleton;
        if(mf != null)
            return mf;
        synchronized(MessageFactory.class) {
            if(singleton == null)
                singleton=createDefaultMessageFactory();
            return singleton;
        }
    }

    public static MessageFactory createDefaultMessageFactory() {
        MessageFactory mf=new MessageFactory();
        return registerDefaultTypes(mf);
    }

    public static MessageFactory registerDefaultTypes(MessageFactory mf) {
        mf.registerDefaultMessage(Message.BYTES_MSG, BytesMessage::new);
        mf.registerDefaultMessage(Message.NIO_MSG, NioMessage::new);
        mf.registerDefaultMessage(Message.EMPTY_MSG, EmptyMessage::new);
        mf.registerDefaultMessage(Message.OBJ_MSG, ObjectMessage::new);
        mf.registerDefaultMessage(Message.LONG_MSG, LongMessage::new);
        mf.registerDefaultMessage(Message.COMPOSITE_MSG, CompositeMessage::new);
        mf.registerDefaultMessage(Message.FRAG_MSG, FragmentedMessage::new);
        mf.registerDefaultMessage(Message.EARLYBATCH_MSG, BatchMessage::new);
        return mf;
    }
    
    /**
     * Creates a message based on the given ID
     * @param type The ID
     * @param <T> The type of the message
     * @return A message
     */
    public <T extends Message> T create(short type) {
        Supplier<? extends Message> creator=type < MIN_TYPE? creators[type] : map.get(type);
        if(creator == null)
            throw new IllegalArgumentException("no creator found for type " + type);
        return (T)creator.get();
    }

    public void registerDefaultMessage(short type, Supplier<? extends Message> generator) {
        Objects.requireNonNull(generator, "the creator must be non-null");
        if(type > MIN_TYPE)
            throw new IllegalArgumentException(String.format("type (%d) must be <= 32", type));
        creators[type]=generator;
    }

    /**
     * Registers a new creator of messages
     * @param type The type associated with the new payload. Needs to be the same in all nodes of the same cluster, and
     *             needs to be available (ie., not taken by JGroups or other applications).
     * @param generator The creator of the payload associated with the given type
     */
    public void register(short type, Supplier<? extends Message> generator) {
        Objects.requireNonNull(generator, "the creator must be non-null");
        if(type < MIN_TYPE)
            throw new IllegalArgumentException(String.format("type (%d) must be >= 32", type));
        if(map.containsKey(type))
            throw new IllegalArgumentException(String.format("type %d is already taken", type));
        map.put(type, generator);
    }

}

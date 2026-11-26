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
    protected static final byte                             MIN_TYPE=32;
    protected static final Supplier<? extends Message>[]    creators=new Supplier[MIN_TYPE];
    protected static Map<Short,Supplier<? extends Message>> map=new HashMap<>();
    static {
        creators[Message.BYTES_MSG]=BytesMessage::new;
        creators[Message.NIO_MSG]=NioMessage::new;
        creators[Message.EMPTY_MSG]=EmptyMessage::new;
        creators[Message.OBJ_MSG]=ObjectMessage::new;
        creators[Message.LONG_MSG]=LongMessage::new;
        creators[Message.COMPOSITE_MSG]=CompositeMessage::new;
        creators[Message.FRAG_MSG]=FragmentedMessage::new;
        creators[Message.EARLYBATCH_MSG]=BatchMessage::new;
    }
    
    /**
     * Creates a message based on the given ID
     * @param type The ID
     * @param <T> The type of the message
     * @return A message
     */
    public static <T extends Message> T create(short type) {
        Supplier<? extends Message> creator=type < MIN_TYPE? creators[type] : map.get(type);
        if(creator == null)
            throw new IllegalArgumentException("no creator found for type " + type);
        return (T)creator.get();
    }

    /**
     * Registers a new creator of messages
     * @param type The type associated with the new payload. Needs to be the same in all nodes of the same cluster, and
     *             needs to be available (ie., not taken by JGroups or other applications).
     * @param generator The creator of the payload associated with the given type
     */
    public static void register(short type, Supplier<? extends Message> generator) {
        Objects.requireNonNull(generator, "the creator must be non-null");
        if(type < MIN_TYPE)
            throw new IllegalArgumentException(String.format("type (%d) must be >= 32", type));
        if(map.containsKey(type))
            throw new IllegalArgumentException(String.format("type %d is already taken", type));
        map.put(type, generator);
    }
}

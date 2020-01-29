package org.jgroups;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Default implementation of {@link MessageFactory}. Uses an array for message IDs less then 32, and a hashmap for
 * types above 32
 * @author Bela Ban
 * @since  5.0
 */
public class DefaultMessageFactory implements MessageFactory {
    protected static final byte                      MIN_TYPE=32;
    protected final Supplier<? extends Message>[]    creators=new Supplier[MIN_TYPE];
    protected Map<Short,Supplier<? extends Message>> map;

    public DefaultMessageFactory() {
        creators[Message.BYTES_MSG]=BytesMessage::new;
        creators[Message.NIO_MSG]=NioMessage::new;
        creators[Message.EMPTY_MSG]=EmptyMessage::new;
        creators[Message.OBJ_MSG]=ObjectMessage::new;
        creators[Message.LONG_MSG]=LongMessage::new;
        creators[Message.COMPOSITE_MSG]=CompositeMessage::new;
        creators[Message.FRAG_MSG]=FragmentedMessage::new;
    }

    public <T extends Message> T create(short type) {
        Supplier<? extends Message> creator=type < MIN_TYPE? creators[type] : map.get(type);
        if(creator == null)
            throw new IllegalArgumentException("no creator found for type " + type);
        return (T)creator.get();
    }

    public void register(short type, Supplier<? extends Message> generator) {
        Objects.requireNonNull(generator, "the creator must be non-null");
        if(type < MIN_TYPE)
            throw new IllegalArgumentException(String.format("type (%d) must be >= 32", type));
        if(map == null)
            map=new HashMap<>();
        if(map.containsKey(type))
            throw new IllegalArgumentException(String.format("type %d is already taken", type));
        map.put(type, generator);
    }
}

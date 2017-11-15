package org.jgroups;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * @author Bela Ban
 * @since  5.0
 */
public class DefaultMessageFactory implements MessageFactory {
    protected final Supplier<? extends Message>[] creators=new Supplier[Byte.MAX_VALUE];
    protected static final byte                   MIN_TYPE=32;

    public DefaultMessageFactory() {
        creators[Message.EMPTY_MSG]=EmptyMessage::new;
        creators[Message.BYTES_MSG]=BytesMessage::new;
        creators[Message.OBJ_MSG]=ObjectMessage::new;
        creators[Message.OBJ_MSG_SERIALIZABLE]=ObjectMessageSerializable::new;
        creators[Message.COMPOSITE_MSG]=CompositeMessage::new;
        creators[Message.FRAG_MSG]=FragmentedMessage::new;
        creators[Message.NIO_MSG]=NioMessage::new;
    }

    public <T extends Message> T create(byte type) {
        Supplier<? extends Message> creator=creators[type];
        if(creator == null)
            throw new IllegalArgumentException("no creator found for type " + type);
        return (T)creator.get();
    }

    public void register(byte type, Supplier<? extends Message> generator) {
        Objects.requireNonNull(generator, "the creator must be non-null");
        if(type <= MIN_TYPE)
            throw new IllegalArgumentException(String.format("type (%d) must be > 32", type));
        if(creators[type] != null)
            throw new IllegalArgumentException(String.format("type %d is already taken", type));
        creators[type]=generator;
    }
}

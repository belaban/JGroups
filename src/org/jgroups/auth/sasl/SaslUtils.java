package org.jgroups.auth.sasl;

import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslServerFactory;
import java.security.Provider;
import java.security.Security;
import java.util.*;

/**
 * Utility methods for handling SASL authentication
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 * @author Tristan Tarrant
 */
public final class SaslUtils {

    private SaslUtils() {
    }

    /**
     * Returns an iterator of all of the registered {@code SaslServerFactory}s where the order is
     * based on the order of the Provider registration and/or class path order. Class path providers
     * are listed before global providers; in the event of a name conflict, the class path provider
     * is preferred.
     *
     * @param classLoader
     *            the class loader to use
     * @param includeGlobal
     *            {@code true} to include globally registered providers, {@code false} to exclude
     *            them
     * @return the {@code Iterator} of {@code SaslServerFactory}s
     */
    public static Iterator<SaslServerFactory> getSaslServerFactories(ClassLoader classLoader, boolean includeGlobal) {
        return getFactories(SaslServerFactory.class, classLoader, includeGlobal);
    }

    /**
     * Returns an iterator of all of the registered {@code SaslServerFactory}s where the order is
     * based on the order of the Provider registration and/or class path order.
     *
     * @return the {@code Iterator} of {@code SaslServerFactory}s
     */
    public static Iterator<SaslServerFactory> getSaslServerFactories() {
        return getFactories(SaslServerFactory.class, null, true);
    }

    /**
     * Returns an iterator of all of the registered {@code SaslClientFactory}s where the order is
     * based on the order of the Provider registration and/or class path order. Class path providers
     * are listed before global providers; in the event of a name conflict, the class path provider
     * is preferred.
     *
     * @param classLoader
     *            the class loader to use
     * @param includeGlobal
     *            {@code true} to include globally registered providers, {@code false} to exclude
     *            them
     * @return the {@code Iterator} of {@code SaslClientFactory}s
     */
    public static Iterator<SaslClientFactory> getSaslClientFactories(ClassLoader classLoader, boolean includeGlobal) {
        return getFactories(SaslClientFactory.class, classLoader, includeGlobal);
    }

    /**
     * Returns an iterator of all of the registered {@code SaslClientFactory}s where the order is
     * based on the order of the Provider registration and/or class path order.
     *
     * @return the {@code Iterator} of {@code SaslClientFactory}s
     */
    public static Iterator<SaslClientFactory> getSaslClientFactories() {
        return getFactories(SaslClientFactory.class, null, true);
    }

    private static <T> Iterator<T> getFactories(Class<T> type, ClassLoader classLoader, boolean includeGlobal) {
        Set<T> factories = new LinkedHashSet<>();
        final ServiceLoader<T> loader = ServiceLoader.load(type, classLoader);
        for (T factory : loader) {
            factories.add(factory);
        }
        if (includeGlobal) {
            Set<String> loadedClasses = new HashSet<>();
            final String filter = type.getSimpleName() + ".";

            Provider[] providers = Security.getProviders();
            for (Provider currentProvider : providers) {
                final ClassLoader cl = currentProvider.getClass().getClassLoader();
                currentProvider.keySet().stream().filter(currentKey -> currentKey instanceof String && ((String)currentKey).startsWith(filter)
                  && ((String)currentKey).indexOf(' ') < 0).forEach(currentKey -> {
                    String className=currentProvider.getProperty((String)currentKey);
                    if(className != null && loadedClasses.add(className)) {
                        try {
                            factories.add(Class.forName(className, true, cl).asSubclass(type).newInstance());
                        } catch(ClassNotFoundException | ClassCastException | InstantiationException | IllegalAccessException e) {
                        }
                    }
                });
            }
        }
        return factories.iterator();
    }

    public static SaslServerFactory getSaslServerFactory(String mech, Map<String, ?> props) {
        Iterator<SaslServerFactory> saslFactories = SaslUtils.getSaslServerFactories(SaslUtils.class.getClassLoader(), true);
        while (saslFactories.hasNext()) {
            SaslServerFactory saslFactory = saslFactories.next();
            for (String supportedMech : saslFactory.getMechanismNames(props)) {
                if (supportedMech.equals(mech)) {
                    return saslFactory;
                }
            }
        }
        throw new IllegalArgumentException("No SASL server factory for mech " + mech);
    }

    public static SaslClientFactory getSaslClientFactory(String mech, Map<String, ?> props) {
        Iterator<SaslClientFactory> saslFactories = SaslUtils.getSaslClientFactories(SaslUtils.class.getClassLoader(), true);
        while (saslFactories.hasNext()) {
            SaslClientFactory saslFactory = saslFactories.next();
            for (String supportedMech : saslFactory.getMechanismNames(props)) {
                if (mech.equals(supportedMech)) {
                    return saslFactory;
                }
            }
        }
        throw new IllegalArgumentException("No SASL client factory for mech " + mech);
    }
}

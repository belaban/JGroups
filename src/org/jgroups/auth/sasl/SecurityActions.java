package org.jgroups.auth.sasl;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Properties;

/**
 * SecurityActions for the org.jgroups.auth.sasl package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 3.6
 */
final class SecurityActions {
	
	private SecurityActions() {
		throw new InstantiationError( "Must not instantiate this class" );
	}

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return action.run();
      }
   }

   static String getSystemProperty(final String name) {
      return doPrivileged(() -> System.getProperty(name));
   }

   public static Properties getSystemProperties() {
      return doPrivileged(System::getProperties);
   }
}

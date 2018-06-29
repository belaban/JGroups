package org.jgroups.auth.sasl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

/**
 * SimpleAuthorizingCallbackHandler. This class implements a simple callback handler which can be
 * used to configure cluster authentication for the JGroups transport. It is configured via system
 * properties. The following properties are available:
 *
 * <ul>
 * <li>sasl.credentials.properties - the path to a property file which contains principal/credential
 * mappings represented as principal=password</li>
 * <li>sasl.local.principal - the name of the principal that is used to identify the local node. It
 * must exist in the sasl.credentials.properties file</li>
 * <li>sasl.roles.properties - (optional) the path to a property file which contains principal/roles
 * mappings represented as principal=role1,role2,role3</li>
 * <li>sasl.role - (optional) if present, authorizes joining nodes only if their principal is
 * <li>sasl.realm - (optional) the name of the realm to use for the SASL mechanisms that require it
 * </li>
 * </ul>
 *
 * @author Tristan Tarrant
 * @since 3.6
 */
public class SimpleAuthorizingCallbackHandler implements CallbackHandler {
    private static final Log log = LogFactory.getLog(SimpleAuthorizingCallbackHandler.class);
    private final Properties credentials;
    private final Properties roles;
    private final Timer timer;
    private final String localPrincipal;
    private final String role;
    private final String realm;

    public SimpleAuthorizingCallbackHandler() {
        this(SecurityActions.getSystemProperties());
    }

    public SimpleAuthorizingCallbackHandler(Properties properties) {
        this.credentials = new Properties();
        this.roles = new Properties();

        localPrincipal = requireProperty(properties, "sasl.local.principal");
        String credentialsFile = requireProperty(properties, "sasl.credentials.properties");
        timer = new Timer();
        File fCredentials = new File(credentialsFile);
        timer.scheduleAtFixedRate(
                new FileWatchTask(fCredentials, new PropertiesReloadFileObserver(fCredentials, credentials)), 0,
                TimeUnit.SECONDS.toMillis(10));
        role = properties.getProperty("sasl.role");
        String rolesFile = properties.getProperty("sasl.roles.properties");
        if (role != null) {
            if (rolesFile == null) {
                throw new IllegalStateException(
                        "To enable role authorization, both sasl.role and sasl.roles.properties system properties must be set");
            } else {
                File fRoles = new File(rolesFile);
                timer.scheduleAtFixedRate(
                        new FileWatchTask(fRoles, new PropertiesReloadFileObserver(fRoles, roles)), 0,
                        TimeUnit.SECONDS.toMillis(10));
            }
        }
        realm = properties.getProperty("sasl.realm");
    }

    private static String requireProperty(Properties properties, String propertyName) {
        String value = properties.getProperty(propertyName);
        if (value == null) {
            throw new IllegalStateException("The required system property " + propertyName + " has not been set");
        } else {
            return value;
        }
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        List<Callback> responseCallbacks = new LinkedList<>();

        String remotePrincipal = null;
        boolean remotePrincipalFound = false;

        for (Callback current : callbacks) {
            if (current instanceof AuthorizeCallback) {
                responseCallbacks.add(current);
            } else if (current instanceof NameCallback) {
                NameCallback nameCallback = (NameCallback) current;
                remotePrincipal = nameCallback.getDefaultName();
                if (remotePrincipal != null) { // server
                    remotePrincipalFound = credentials.containsKey(remotePrincipal);
                } else { // client, we need to respond
                    responseCallbacks.add(current);
                }
            } else if (current instanceof PasswordCallback) {
                responseCallbacks.add(current);
            } else if (current instanceof RealmCallback) {
                String realmLocal = ((RealmCallback) current).getDefaultText();
                if (realmLocal != null && !realmLocal.equals(this.realm)) {
                    throw new IOException("Invalid realm " + realmLocal);
                }
                responseCallbacks.add(current);
            } else {
                throw new UnsupportedCallbackException(current);
            }
        }

        for (Callback current : responseCallbacks) {
            if (current instanceof NameCallback) {
                ((NameCallback) current).setName(localPrincipal);
            } else if (current instanceof AuthorizeCallback) {
                AuthorizeCallback acb = (AuthorizeCallback) current;
                String authenticationId = acb.getAuthenticationID();
                String authorizationId = acb.getAuthorizationID();
                acb.setAuthorized(authenticationId.equals(authorizationId));
                if (role != null) {
                    String principalRoleNames = roles.getProperty(acb.getAuthorizationID());
                    List<String> principalRoles = (List<String>) (principalRoleNames != null
                            ? Arrays.asList(principalRoleNames.split("\\s*,\\s*")) : Collections.emptyList());
                    if (!principalRoles.contains(role)) {
                        throw new IOException("Unauthorized user " + authorizationId);
                    }
                }
            } else if (current instanceof PasswordCallback) {
                String password;
                if (remotePrincipal == null) { // client, send our password
                    password = credentials.getProperty(localPrincipal);
                } else if (remotePrincipalFound) { // server, validate incoming password
                    password = credentials.getProperty(remotePrincipal);
                } else {
                    throw new IOException("Unauthorized user " + remotePrincipal);
                }
                ((PasswordCallback) current).setPassword(password.toCharArray());
            } else if (current instanceof RealmCallback) {
                ((RealmCallback)current).setText(realm);
            }
        }
    }

    public static class PropertiesReloadFileObserver implements FileObserver {

        private final Properties properties;

        PropertiesReloadFileObserver(File file, Properties properties) {
            this.properties = properties;
            loadProperties(file);
        }

        private void loadProperties(File file) {
            FileInputStream fis = null;
            try {
                fis = new FileInputStream(file);
                properties.load(fis);
            } catch (IOException e) {
                log.error(Util.getMessage("AnErrorOccurredWhileLoadingPropertiesFrom") + file, e);
            } finally {
                Util.close(fis);
            }
        }

        @Override
        public void fileChanged(File file) {
            loadProperties(file);
        }
    }
}

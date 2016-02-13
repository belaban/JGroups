
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Property;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Responses;
import org.jgroups.util.Util;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.util.*;

/**
 * Discovery protocol based on Openstack Swift (object storage).
 * <p/>
 * This implementation is derived from Gustavo Fernandes work on RACKSPACE_PING
 *
 * @author tsegismont
 * @since 3.1
 */
@Experimental
public class SWIFT_PING extends FILE_PING {

    private static final Log log = LogFactory.getLog(SWIFT_PING.class);

    protected SwiftClient swiftClient = null;

    @Property(description = "Authentication url")
    protected String auth_url = null;

    @Property(description = "Authentication type")
    protected String auth_type = "keystone_v_2_0";

    @Property(description = "Openstack Keystone tenant name")
    protected String tenant = null;

    @Property(description = "Username")
    protected String username = null;

    @Property(description = "Password",exposeAsManagedAttribute=false)
    protected String password = null;

    @Property(description = "Name of the root container")
    protected String container = "jgroups";

    @Override
    public void init() throws Exception {
        Utils.validateNotEmpty(auth_url, "auth_url");
        Utils.validateNotEmpty(auth_type, "auth_type");
        Utils.validateNotEmpty(username, "username");
        Utils.validateNotEmpty(password, "password");
        Utils.validateNotEmpty(container, "container");

        Authenticator authenticator = createAuthenticator();
        authenticator.validateParams();

        swiftClient = new SwiftClient(authenticator);
        // Authenticate now to record credential
        swiftClient.authenticate();

        super.init();
    }

    private Authenticator createAuthenticator() throws Exception {

        AUTH_TYPE authType = AUTH_TYPE.getByConfigName(auth_type);
        if (authType == null) {
            throw new IllegalArgumentException("Invalid 'auth_type' : "
                                                 + auth_type);
        }

        URL authUrl = new URL(auth_url);
        Authenticator authenticator = null;
        switch (authType) {
            case KEYSTONE_V_2_0:
                authenticator = new Keystone_V_2_0_Auth(tenant, authUrl, username,
                                                        password);
                break;

            default:
                // We shouldn't come here since we checked auth_type
                throw new IllegalStateException("Could not select authenticator");
        }
        return authenticator;
    }

    @Override
    protected void createRootDir() {
        try {
            swiftClient.createContainer(container);
        } catch (Exception e) {
            log.error(Util.getMessage("FailureCreatingContainer"), e);
        }
    }

    @Override
    protected void readAll(List<Address> members, String clustername, Responses responses) {
        try {
            List<String> objects = swiftClient.listObjects(container);
            for(String object: objects) {
                List<PingData> list=null;
                byte[] bytes = swiftClient.readObject(container, object);
                if((list=read(new ByteArrayInputStream(bytes))) == null) {
                    log.warn("failed reading " + object);
                    continue;
                }
                for(PingData data: list) {
                    if(members == null || members.contains(data.getAddress()))
                        responses.addResponse(data, data.isCoord());
                    if(local_addr != null && !local_addr.equals(data.getAddress()))
                        addDiscoveryResponseToCaches(data.getAddress(), data.getLogicalName(), data.getPhysicalAddr());
                }
            }

        } catch (Exception e) {
            log.error(Util.getMessage("ErrorUnmarshallingObject"), e);
        }
    }

    @Override
    protected void write(List<PingData> list, String clustername) {
        try {
            String filename = clustername + "/" + addressToFilename(local_addr);
            ByteArrayOutputStream out=new ByteArrayOutputStream(4096);
            write(list, out);
            byte[] data=out.toByteArray();
            swiftClient.createObject(container, filename, data);
        } catch (Exception e) {
            log.error(Util.getMessage("ErrorMarshallingObject"), e);
        }
    }


    @Override
    protected void remove(String clustername, Address addr) {
        String fileName = clustername + "/" + addressToFilename(addr);
        try {
            swiftClient.deleteObject(container, fileName);
        } catch (Exception e) {
            log.error(Util.getMessage("FailureRemovingData"), e);
        }
    }


    @Override
    protected void removeAll(String clustername) {
        try {
            List<String> objects=swiftClient.listObjects(container);
            for(String objName : objects) {
                swiftClient.deleteObject(container, objName);
            }
        }
        catch(Exception t) {
            log.error(Util.getMessage("FailedRemovingObjects"), t);
        }
    }



    private static class HttpHeaders {

        private static final String CONTENT_TYPE_HEADER = "Content-type";

        private static final String ACCEPT_HEADER = "Accept";

        //
        // private static final String AUTH_HEADER = "X-Auth-User";
        //
        // private static final String AUTH_KEY_HEADER = "X-Auth-Key";
        //
        private static final String STORAGE_TOKEN_HEADER = "X-Storage-Token";

        //
        // private static final String STORAGE_URL_HEADER = "X-Storage-Url";

        private static final String CONTENT_LENGTH_HEADER = "Content-Length";
    }

    /**
     * Supported Swift authentication providers
     */
    private enum AUTH_TYPE {

        KEYSTONE_V_2_0("keystone_v_2_0");

        private static final Map<String, AUTH_TYPE> LOOKUP = new HashMap<>();

        static {
            for (AUTH_TYPE type : EnumSet.allOf(AUTH_TYPE.class))
                LOOKUP.put(type.configName, type);
        }

        private String configName;

        private AUTH_TYPE(String externalName) {
            this.configName = externalName;
        }

        public static AUTH_TYPE getByConfigName(String configName) {
            return LOOKUP.get(configName);
        }

    }

    /**
     * Result of a successfully authenticated session
     */
    private static class Credentials {

        private final String authToken;

        private final String storageUrl;

        public Credentials(String authToken, String storageUrl) {
            this.authToken = authToken;
            this.storageUrl = storageUrl;
        }
    }

    /**
     * Contract for Swift authentication providers
     */
    private static interface Authenticator {

        /**
         * Validate SWIFT_PING config parameters
         */
        void validateParams();

        Credentials authenticate() throws Exception;
    }

    /**
     * Openstack Keytsone v2.0 authentication provider. Thread safe
     * implementation
     */
    private static class Keystone_V_2_0_Auth implements Authenticator {
        // Using Java's built-in JavaScript engine to parse JSON response. We use
        // this approach to avoid introducing an external dependency on a JSON parser.
        private final static String JSON_RESPONSE_PARSING_SCRIPT = 
                "var response = JSON.parse(json);" + 
                "var result = {};" + 
                "result.id = response.access.token.id;" + 
                "var serviceCatalog = response.access.serviceCatalog;" + 
                "for (var i = 0; i < serviceCatalog.length; i++) {" + 
                "    var service = serviceCatalog[i];" + 
                "    if (service.type == \"object-store\") {" + 
                "        result.url = service.endpoints[0].publicURL;" + 
                "        break;" + 
                "    }" + 
                "}" + 
                "result;";
        
        private static Object scriptEngineLock = new Object();
        private static ScriptEngine scriptEngine;
        
        private final String tenant;

        private final URL authUrl;

        private final String username;

        private final String password;

        public Keystone_V_2_0_Auth(String tenant, URL authUrl, String username,
                                   String password) {
            this.tenant = tenant;
            this.authUrl = authUrl;
            this.username = username;
            this.password = password;
        }

        public void validateParams() {
            // All others params already validated
            Utils.validateNotEmpty(tenant, "tenant");
        }

        public Credentials authenticate() throws Exception {
            HttpURLConnection urlConnection = new ConnBuilder(authUrl)
                    .addHeader(HttpHeaders.CONTENT_TYPE_HEADER,
                            "application/json")
                    .addHeader(HttpHeaders.ACCEPT_HEADER, "application/json")
                    .getConnection();

            StringBuilder jsonBuilder = new StringBuilder();
            jsonBuilder.append("{\"auth\": {\"tenantName\": \"").append(tenant)
                    .append("\", \"passwordCredentials\": {\"username\": \"")
                    .append(username).append("\", \"password\": \"")
                    .append(password).append("\"}}}");

            HttpResponse response = Utils.doOperation(urlConnection,
                    jsonBuilder.toString().getBytes(), true);

            if (response.isSuccessCode()) {
                Map<String, String> result = parseJsonResponse(new String(response.payload, "UTF-8"));

                String authToken = result.get("id");
                String storageUrl = result.get("url");
                if (authToken == null)
                {
                    throw new IllegalStateException("Missing token id in authentication response");
                }
                if (storageUrl == null)
                {
                    throw new IllegalStateException("Missing storage service URL in authentication response");
                }
                
                log.trace("Authentication successful");
                return new Credentials(authToken, storageUrl);
            } else {
                throw new IllegalStateException(
                        "Error authenticating to the service. Please check your credentials. Code = "
                                + response.code);
            }
        }
        
        protected Map<String,String> parseJsonResponse(String json) throws ScriptException
        {
            synchronized (scriptEngineLock)
            {
                if (scriptEngine == null)
                {
                    scriptEngine = new ScriptEngineManager().getEngineByName("JavaScript");
                    if (scriptEngine == null) {
                        throw new RuntimeException("Failed to load JavaScript script engine");
                    }
                 
                }
                Bindings bindings = new SimpleBindings();
                bindings.put("json", json);
                
                return (Map<String, String>)scriptEngine.eval(JSON_RESPONSE_PARSING_SCRIPT, bindings);
            }
        }
    }

    /**
     * Build HttpURLConnections with adequate headers and method
     */
    private static class ConnBuilder {

        private HttpURLConnection con;

        public ConnBuilder(URL url) {
            try {
                con = (HttpURLConnection) url.openConnection();
            } catch (IOException e) {
                log.error(Util.getMessage("ErrorBuildingURL"), e);
            }
        }

        public ConnBuilder(Credentials credentials, String container,
                           String object) {
            try {
                String url = credentials.storageUrl + "/" + container;
                if (object != null) {
                    url = url + "/" + object;
                }
                con = (HttpURLConnection) new URL(url).openConnection();
            } catch (IOException e) {
                log.error(Util.getMessage("ErrorCreatingConnection"), e);
            }

        }

        public ConnBuilder method(String method) {
            try {
                con.setRequestMethod(method);
            } catch (ProtocolException e) {
                log.error(Util.getMessage("ProtocolError"), e);
            }
            return this;
        }

        public ConnBuilder addHeader(String key, String value) {
            con.setRequestProperty(key, value);
            return this;
        }

        public HttpURLConnection getConnection() {
            return con;
        }
    }

    /**
     * Response for a Swift API call
     */
    private static class HttpResponse {

        // For later use
        private final Map<String, List<String>> headers;

        private final int code;

        private final byte[] payload;

        HttpResponse(Map<String, List<String>> headers, int code, byte[] payload) {
            this.headers = headers;
            this.code = code;
            this.payload = payload;
        }

        public List<String> payloadAsLines() {
            List<String> lines = new ArrayList<>();
            BufferedReader in;
            try {
                String line;
                in = new BufferedReader(new InputStreamReader(
                        new ByteArrayInputStream(payload)));

                while ((line = in.readLine()) != null) {
                    lines.add(line);
                }
                in.close();
            } catch (IOException e) {
                log.error(Util.getMessage("ErrorReadingObjects"), e);
            }
            return lines;
        }

        public boolean isSuccessCode() {
            return Utils.isSuccessCode(code);
        }

        public boolean isAuthDenied() {
            return Utils.isAuthDenied(code);
        }
    }

    /**
     * A thread safe Swift client
     */
    protected static class SwiftClient {

        private final Authenticator authenticator;

        private volatile Credentials credentials = null;

        /**
         * Constructor
         *
         * @param authenticator Swift auth provider
         */
        public SwiftClient(Authenticator authenticator) {
            this.authenticator = authenticator;
        }

        /**
         * Authenticate
         *
         * @throws Exception
         */
        public void authenticate() throws Exception {
            credentials = authenticator.authenticate();
        }

        /**
         * Delete a object (=file) from the storage
         *
         * @param containerName Folder name
         * @param objectName    File name
         * @throws IOException
         */
        public void deleteObject(String containerName, String objectName)
                throws Exception {
            HttpURLConnection urlConnection = getConnBuilder(containerName,
                    objectName).method("DELETE").getConnection();

            HttpResponse response = Utils.doVoidOperation(urlConnection);

            if (!response.isSuccessCode()) {
                if (response.isAuthDenied()) {
                    log.warn("Refreshing credentials and retrying");
                    authenticate();
                    deleteObject(containerName, objectName);
                } else {
                    log.error(Util.getMessage("ErrorDeletingObject") + objectName
                            + " from container " + containerName + ",code = "
                            + response.code);
                }
            }

        }

        /**
         * Create a container, which is equivalent to a bucket
         *
         * @param containerName Name of the container
         * @throws IOException
         */
        public void createContainer(String containerName) throws Exception {
            HttpURLConnection urlConnection = getConnBuilder(containerName,
                    null).method("PUT").getConnection();

            HttpResponse response = Utils.doVoidOperation(urlConnection);

            if (!response.isSuccessCode()) {
                if (response.isAuthDenied()) {
                    log.warn("Refreshing credentials and retrying");
                    authenticate();
                    createContainer(containerName);
                } else {
                    log.error(Util.getMessage("ErrorCreatingContainer") + containerName
                            + " ,code = " + response.code);
                }
            }
        }

        /**
         * Create an object (=file)
         *
         * @param containerName Name of the container
         * @param objectName    Name of the file
         * @param contents      Binary content of the file
         * @throws IOException
         */
        public void createObject(String containerName, String objectName,
                                 byte[] contents) throws Exception {
            HttpURLConnection conn = getConnBuilder(containerName, objectName)
                    .method("PUT")
                    .addHeader(HttpHeaders.CONTENT_LENGTH_HEADER,
                            String.valueOf(contents.length)).getConnection();

            HttpResponse response = Utils.doSendOperation(conn, contents);

            if (!response.isSuccessCode()) {
                if (response.isAuthDenied()) {
                    log.warn("Refreshing credentials and retrying");
                    authenticate();
                    createObject(containerName, objectName, contents);
                } else {
                    log.error(Util.getMessage("ErrorCreatingObject") + objectName
                            + " in container " + containerName + ",code = "
                            + response.code);
                }
            }

        }

        /**
         * Read the content of a file
         *
         * @param containerName Name of the folder
         * @param objectName    name of the file
         * @return Content of the files
         * @throws IOException
         */
        public byte[] readObject(String containerName, String objectName)
                throws Exception {
            HttpURLConnection urlConnection = getConnBuilder(containerName,
                    objectName).getConnection();

            HttpResponse response = Utils.doReadOperation(urlConnection);

            if (!response.isSuccessCode()) {
                if (response.isAuthDenied()) {
                    log.warn("Refreshing credentials and retrying");
                    authenticate();
                    return readObject(containerName, objectName);
                } else {
                    log.error(Util.getMessage("ErrorReadingObject") + objectName
                            + " from container " + containerName + ", code = "
                            + response.code);
                }
            }
            return response.payload;

        }

        /**
         * List files in a folder
         *
         * @param containerName Folder name
         * @return List of file names
         * @throws IOException
         */
        public List<String> listObjects(String containerName) throws Exception {
            HttpURLConnection urlConnection = getConnBuilder(containerName,
                    null).getConnection();

            HttpResponse response = Utils.doReadOperation(urlConnection);

            if (!response.isSuccessCode()) {
                if (response.isAuthDenied()) {
                    log.warn("Refreshing credentials and retrying");
                    authenticate();
                    return listObjects(containerName);
                } else {
                    log.error(Util.getMessage("ErrorListingContainer") + containerName
                            + ", code = " + response.code);
                }

            }
            return response.payloadAsLines();
        }

        private ConnBuilder getConnBuilder(String container, String object) {
            ConnBuilder connBuilder = new ConnBuilder(credentials, container,
                    object);
            connBuilder.addHeader(HttpHeaders.STORAGE_TOKEN_HEADER,
                    credentials.authToken);
            connBuilder.addHeader(HttpHeaders.ACCEPT_HEADER, "*/*");
            return connBuilder;
        }

    }

    private static class Utils {

        public static void validateNotEmpty(String arg, String argname) {
            if (arg == null || arg.trim().length() == 0) {
                throw new IllegalArgumentException("'" + argname
                        + "' cannot be empty");
            }
        }

        /**
         * Is http response code in success range ?
         *
         * @param code
         * @return
         */
        public static boolean isSuccessCode(int code) {
            return code >= 200 && code < 300;
        }

        /**
         * Is http Unauthorized response code ?
         *
         * @param code
         * @return
         */
        public static boolean isAuthDenied(int code) {
            return code == 401;
        }

        /**
         * Do a http operation
         *
         * @param urlConnection the HttpURLConnection to be used
         * @param inputData     if not null,will be written to the urlconnection.
         * @param hasOutput     if true, read content back from the urlconnection
         * @return Response
         * @throws IOException
         */
        public static HttpResponse doOperation(HttpURLConnection urlConnection,
                                               byte[] inputData, boolean hasOutput) throws IOException {
            HttpResponse response = null;
            InputStream inputStream = null;
            OutputStream outputStream = null;
            byte[] payload = null;
            try {
                if (inputData != null) {
                    urlConnection.setDoOutput(true);
                    outputStream = urlConnection.getOutputStream();
                    outputStream.write(inputData);
                }
                /*
             * Get response code first. HttpURLConnection does not allow to
             * read inputstream if response code is not success code
             */
                int responseCode = urlConnection.getResponseCode();
                if (hasOutput && isSuccessCode(responseCode)) {
                    payload = getBytes(urlConnection.getInputStream());
                }
                response = new HttpResponse(urlConnection.getHeaderFields(),
                        responseCode, payload);
            } finally {
                Util.close(inputStream);
                Util.close(outputStream);
            }
            return response;
        }

        /**
         * Get bytes of this {@link InputStream}
         *
         * @param inputStream
         * @return
         * @throws IOException
         */
        public static byte[] getBytes(InputStream inputStream)
                throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int len;
            for (; ; ) {
                len = inputStream.read(buffer);
                if (len == -1) {
                    break;
                }
                baos.write(buffer, 0, len);
            }
            return baos.toByteArray();
        }

        /**
         * Do a operation that does not write or read from HttpURLConnection,
         * except for the headers
         *
         * @param urlConnection the connection
         * @return Response
         * @throws IOException
         */
        public static HttpResponse doVoidOperation(
                HttpURLConnection urlConnection) throws IOException {
            return doOperation(urlConnection, null, false);
        }

        /**
         * Do a operation that writes content to the HttpURLConnection
         *
         * @param urlConnection the connection
         * @param content       The content to send
         * @return Response
         * @throws IOException
         */
        public static HttpResponse doSendOperation(
                HttpURLConnection urlConnection, byte[] content)
                throws IOException {
            return doOperation(urlConnection, content, false);
        }

        /**
         * Do a operation that reads from the httpconnection
         *
         * @param urlConnection The connections
         * @return Response
         * @throws IOException
         */
        public static HttpResponse doReadOperation(
                HttpURLConnection urlConnection) throws IOException {
            return doOperation(urlConnection, null, true);
        }

    }

}

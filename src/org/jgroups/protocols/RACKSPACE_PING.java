package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.annotations.Property;
import org.jgroups.logging.Log;
import org.jgroups.util.Responses;
import org.jgroups.util.Util;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Discovery protocol based on Rackspace Cloud Files storage solution
 *
 * @author Gustavo Fernandes
 */
public class RACKSPACE_PING extends FILE_PING {
    private static final String UKService = "https://lon.auth.api.rackspacecloud.com/v1.0";
    private static final String USService = "https://auth.api.rackspacecloud.com/v1.0";

    protected RackspaceClient rackspaceClient;

    @Property(description = "Rackspace username")
    protected String username;

    @Property(description = "Rackspace API access key",exposeAsManagedAttribute=false)
    protected String apiKey;

    @Property(description = "Rackspace region, either UK or US")
    protected String region;

    @Property(description = "Name of the root container")
    protected String container = "jgroups";

    @Override
    public void init() throws Exception {
        if (username == null) {
            throw new IllegalArgumentException("Rackspace 'username' must not be null");
        }
        if (apiKey == null) {
            throw new IllegalArgumentException("Rackspace 'apiKey' must not be null");
        }
        if (region == null || (!region.equals("UK") && !region.equals("US"))) {
            throw new IllegalArgumentException("Invalid 'region', must be UK or US");
        }

        URL authURL = new URL(region.equals("UK") ? UKService : USService);
        rackspaceClient = new RackspaceClient(authURL, username, apiKey).log(log);

        super.init();

    }


    @Override
    protected void createRootDir() {
        rackspaceClient.authenticate();
        rackspaceClient.createContainer(container);
    }


    @Override
    protected void readAll(List<Address> members, String clustername, Responses responses) {
        try {
            List<String> objects = rackspaceClient.listObjects(container);
            for(String object: objects) {
                List<PingData> list=null;
                byte[] bytes = rackspaceClient.readObject(container, object);
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
            rackspaceClient.createObject(container, filename, data);
        } catch (Exception e) {
            log.error(Util.getMessage("ErrorMarshallingObject"), e);
        }
    }


    @Override
    protected void remove(String clustername, Address addr) {
        String fileName = clustername + "/" + addressToFilename(addr);
        rackspaceClient.deleteObject(container, fileName);
    }

    @Override
    protected void removeAll(String clustername) {
        List<String> objects = rackspaceClient.listObjects(container);
        for(String objName: objects) {
            rackspaceClient.deleteObject(container, objName);
        }
    }

    /**
     * A thread safe Rackspace ReST client
     */
    protected static class RackspaceClient {
        private static final String ACCEPT_HEADER = "Accept";
        private static final String AUTH_HEADER = "X-Auth-User";
        private static final String AUTH_KEY_HEADER = "X-Auth-Key";
        private static final String STORAGE_TOKEN_HEADER = "X-Storage-Token";
        private static final String STORAGE_URL_HEADER = "X-Storage-Url";
        private static final String CONTENT_LENGTH_HEADER = "Content-Length";
        private final URL apiEndpoint;
        private final String username;
        private final String apiKey;

        private volatile Credentials credentials;
        private Log                  log;

        /**
         * Constructor
         *
         * @param apiEndpoint UK or US authentication endpoint
         * @param username    Rackspace username
         * @param apiKey      Rackspace apiKey
         */
        public RackspaceClient(URL apiEndpoint, String username, String apiKey) {
            this.apiEndpoint = apiEndpoint;
            this.username = username;
            this.apiKey = apiKey;
        }

        RackspaceClient log(Log l) {
            this.log=l; return this;
        }

        /**
         * Authenticate
         */
        public void authenticate() {

            HttpURLConnection urlConnection = new ConnBuilder(apiEndpoint)
                    .addHeader(AUTH_HEADER, username)
                    .addHeader(AUTH_KEY_HEADER, apiKey)
                    .getConnection();

            Response response = doAuthOperation(urlConnection);

            if (response.isSuccessCode()) {
                credentials = new Credentials(
                        response.getHeader(STORAGE_TOKEN_HEADER),
                        response.getHeader(STORAGE_URL_HEADER)
                );

                log.trace("Authentication successful");
            } else {
                throw new IllegalStateException("Error authenticating to the service. Please check your credentials. Code = " + response.code);
            }

        }

        /**
         * Delete a object (=file) from the storage
         *
         * @param containerName Folder name
         * @param objectName    File name
         */
        public void deleteObject(String containerName, String objectName) {
            HttpURLConnection urlConnection = new ConnBuilder(credentials, containerName, objectName)
                    .method("DELETE")
                    .getConnection();

            Response response = doVoidOperation(urlConnection);

            if (!response.isSuccessCode()) {
                if (response.isAuthDenied()) {
                    log.warn("Refreshing credentials and retrying");
                    authenticate();
                    deleteObject(containerName, objectName);
                } else {
                    log.error(Util.getMessage("ErrorDeletingObject") + objectName + " from container " + containerName + ",code = " + response.code);
                }
            }

        }

        /**
         * Create a container, which is equivalent to a bucket
         *
         * @param containerName Name of the container
         */
        public void createContainer(String containerName) {
            HttpURLConnection urlConnection = new ConnBuilder(credentials, containerName, null)
                    .method("PUT")
                    .getConnection();

            Response response = doVoidOperation(urlConnection);

            if (!response.isSuccessCode()) {
                if (response.isAuthDenied()) {
                    log.warn("Refreshing credentials and retrying");
                    authenticate();
                    createContainer(containerName);
                } else {
                    log.error(Util.getMessage("ErrorCreatingContainer") + containerName + " ,code = " + response.code);
                }
            }
        }

        /**
         * Create an object (=file)
         *
         * @param containerName Name of the container
         * @param objectName    Name of the file
         * @param contents      Binary content of the file
         */
        public void createObject(String containerName, String objectName, byte[] contents) {
            HttpURLConnection conn = new ConnBuilder(credentials, containerName, objectName)
                    .method("PUT")
                    .addHeader(CONTENT_LENGTH_HEADER, String.valueOf(contents.length))
                    .getConnection();

            Response response = doSendOperation(conn, contents);

            if (!response.isSuccessCode()) {
                if (response.isAuthDenied()) {
                    log.warn("Refreshing credentials and retrying");
                    authenticate();
                    createObject(containerName, objectName, contents);
                } else {
                    log.error(Util.getMessage("ErrorCreatingObject") + objectName + " in container " + containerName + ",code = " + response.code);
                }
            }

        }

        /**
         * Read the content of a file
         *
         * @param containerName Name of the folder
         * @param objectName    name of the file
         * @return Content of the files
         */
        public byte[] readObject(String containerName, String objectName) {
            HttpURLConnection urlConnection = new ConnBuilder(credentials, containerName, objectName).getConnection();

            Response response = doReadOperation(urlConnection);

            if (!response.isSuccessCode()) {
                if (response.isAuthDenied()) {
                    log.warn("Refreshing credentials and retrying");
                    authenticate();
                    return readObject(containerName, objectName);
                } else {
                    log.error(Util.getMessage("ErrorReadingObject") + objectName + " from container " + containerName + ", code = " + response.code);
                }
            }
            return response.payload;

        }

        /**
         * List files in a folder
         *
         * @param containerName Folder name
         * @return List of file names
         */
        public List<String> listObjects(String containerName) {
            HttpURLConnection urlConnection = new ConnBuilder(credentials, containerName, null).getConnection();

            Response response = doReadOperation(urlConnection);

            if (!response.isSuccessCode()) {
                if (response.isAuthDenied()) {
                    log.warn("Refreshing credentials and retrying");
                    authenticate();
                    return listObjects(containerName);
                } else {
                    log.error(Util.getMessage("ErrorListingContainer") + containerName + ", code = " + response.code);
                }

            }
            return response.payloadAsLines();
        }

        /**
         * Do a http operation
         *
         * @param urlConnection the HttpURLConnection to be used
         * @param inputData     if not null,will be written to the urlconnection.
         * @param hasOutput     if true, read content back from the urlconnection
         * @return Response
         */
        private Response doOperation(HttpURLConnection urlConnection, byte[] inputData, boolean hasOutput) {
            Response response = null;
            InputStream inputStream = null;
            OutputStream outputStream = null;
            byte[] payload = null;
            try {
                if (inputData != null) {
                    urlConnection.setDoOutput(true);
                    outputStream = urlConnection.getOutputStream();
                    outputStream.write(inputData);
                }
                if (hasOutput) {
                    inputStream = urlConnection.getInputStream();
                    payload = Util.readFileContents(urlConnection.getInputStream());
                }
                response = new Response(urlConnection.getHeaderFields(), urlConnection.getResponseCode(), payload);

            } catch (IOException e) {
                log.error(Util.getMessage("ErrorCallingService"), e);
            } finally {
                Util.close(inputStream);
                Util.close(outputStream);
            }
            return response;

        }

        /**
         * Do a http auth operation, will not handle 401 permission denied errors
         *
         * @param urlConnection the HttpURLConnection to be used
         * @return Response  Response
         */
        private Response doAuthOperation(HttpURLConnection urlConnection) {
            return doOperation(urlConnection, null, false);
        }

        /**
         * Do a operation that does not write or read from HttpURLConnection, except for the headers
         *
         * @param urlConnection the connection
         * @return Response
         */
        private Response doVoidOperation(HttpURLConnection urlConnection) {
            return doOperation(urlConnection, null, false);
        }

        /**
         * Do a operation that writes content to the HttpURLConnection
         *
         * @param urlConnection the connection
         * @param content       The content to send
         * @return Response
         */
        private Response doSendOperation(HttpURLConnection urlConnection, byte[] content) {
            return doOperation(urlConnection, content, false);
        }

        /**
         * Do a operation that reads from the httpconnection
         *
         * @param urlConnection The connections
         * @return Response
         */
        private Response doReadOperation(HttpURLConnection urlConnection) {
            return doOperation(urlConnection, null, true);
        }

        /**
         * Build HttpURLConnections with adequate headers and method
         */
        private class ConnBuilder {

            private HttpURLConnection con;

            public ConnBuilder(URL url) {
                try {
                    con = (HttpURLConnection) url.openConnection();
                } catch (IOException e) {
                    log.error(Util.getMessage("ErrorBuildingURL"), e);
                }
            }

            public ConnBuilder(Credentials credentials, String container, String object) {
                try {
                    String url = credentials.storageURL + "/" + container;
                    if (object != null) {
                        url = url + "/" + object;
                    }
                    con = (HttpURLConnection) new URL(url).openConnection();
                    con.addRequestProperty(STORAGE_TOKEN_HEADER, credentials.authToken);
                    con.addRequestProperty(ACCEPT_HEADER, "*/*");
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
         * Result of an successfully authenticated session
         */
        private class Credentials {
            private final String authToken;
            private final String storageURL;

            public Credentials(String authToken, String storageURL) {
                this.authToken = authToken;
                this.storageURL = storageURL;
            }

        }

        /**
         * Response for a Rackspace API call
         */
        private class Response {
            private final Map<String, List<String>> headers;
            private final int code;
            private final byte[] payload;

            Response(Map<String, List<String>> headers, int code, byte[] payload) {
                this.headers = headers;
                this.code = code;
                this.payload = payload;
            }

            private String getHeader(String name) {
                return headers.get(name).get(0);
            }

            public boolean isSuccessCode() {
                return code >= 200 && code < 300;
            }

            public boolean isAuthDenied() {
                return code == 401;
            }

            public List<String> payloadAsLines() {
                List<String> lines = new ArrayList<>();
                BufferedReader in;
                try {
                    String line;
                    in = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(payload)));

                    while ((line = in.readLine()) != null) {
                        lines.add(line);
                    }
                    in.close();
                } catch (IOException e) {
                    log.error(Util.getMessage("ErrorReadingObjects"), e);
                }
                return lines;
            }
        }

    }

}

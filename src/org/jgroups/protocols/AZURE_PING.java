package org.jgroups.protocols;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Constructor;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.InvalidParameterException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.EventListener;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import org.jgroups.Address;
import org.jgroups.annotations.Property;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Responses;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Implementation of PING protocol for AZURE using Storage Blobs. See doc/design/AZURE_PING.md for design.
 *
 * @author Radoslav Husar
 * @version Jun 2015
 */
public class AZURE_PING extends FILE_PING {

    private static final Log log = LogFactory.getLog(AZURE_PING.class);

    @Property(description = "The name of the storage account.")
    protected String storage_account_name;

    @Property(description = "The secret account access key.", exposeAsManagedAttribute = false)
    protected String storage_access_key;

    @Property(description = "Container to store ping information in. Must be valid DNS name.")
    protected String container;

    @Property(description = "Whether or not to use HTTPS to connect to Azure.")
    protected boolean use_https = true;

    public static final int STREAM_BUFFER_SIZE = 4096;

    private CloudBlobContainer containerReference;

    @Override
    public void init() throws Exception {
        super.init();

        // Validate configuration
        // Can throw IAEs
        this.validateConfiguration();

        try {
            StorageCredentials credentials = new StorageCredentialsAccountAndKey(storage_account_name, storage_access_key);
            CloudStorageAccount storageAccount = new CloudStorageAccount(credentials, use_https);
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
            containerReference = blobClient.getContainerReference(container);
            boolean created = containerReference.createIfNotExists();

            if (created) {
                log.info("Created container named '%s'.", container);
            } else {
                log.debug("Using existing container named '%s'.", container);
            }

        } catch (Exception ex) {
            log.error("Error creating a storage client! Check your configuration.", ex);
        }
    }

    public void validateConfiguration() throws IllegalArgumentException {
        // Validate that container name is configured and must be all lowercase
        if (container == null || !container.toLowerCase().equals(container) || container.contains("--")
                || container.startsWith("-") || container.length() < 3 || container.length() > 63) {
            throw new IllegalArgumentException("Container name must be configured and must meet Azure requirements (must be a valid DNS name).");
        }
        // Account name and access key must be both configured for write access
        if (storage_account_name == null || storage_access_key == null) {
            throw new IllegalArgumentException("Account name and key must be configured.");
        }
        // Lets inform users here that https would be preferred
        if (!use_https) {
            log.info("Configuration is using HTTP, consider switching to HTTPS instead.");
        }

    }

    @Override
    protected void createRootDir() {
        // Do not remove this!
        // There is no root directory to create, overriding here with noop.
    }

    @Override
    protected void readAll(final List<Address> members, final String clustername, final Responses responses) {
        if (clustername == null) {
            return;
        }

        String prefix = sanitize(clustername);

        Iterable<ListBlobItem> listBlobItems = containerReference.listBlobs(prefix);
        for (ListBlobItem blobItem : listBlobItems) {
            try {
                // If the item is a blob and not a virtual directory.
                // n.b. what an ugly API this is
                if (blobItem instanceof CloudBlob) {
                    CloudBlob blob = (CloudBlob) blobItem;
                    ByteArrayOutputStream os = new ByteArrayOutputStream(STREAM_BUFFER_SIZE);
                    blob.download(os);
                    byte[] pingBytes = os.toByteArray();
                    parsePingData(pingBytes, members, responses);
                }
            } catch (Exception t) {
                log.error("Error fetching ping data.");
            }
        }
    }

    protected void parsePingData(final byte[] pingBytes, final List<Address> members, final Responses responses) {
        if (pingBytes == null || pingBytes.length <= 0) {
            return;
        }
        List<PingData> list;
        try {
            list = read(new ByteArrayInputStream(pingBytes));
            if (list != null) {
                // This is a common piece of logic for all PING protocols copied from org/jgroups/protocols/FILE_PING.java:245
                // Maybe could be extracted for all PING impls to share this logic?
                for (PingData data : list) {
                    if (members == null || members.contains(data.getAddress())) {
                        responses.addResponse(data, data.isCoord());
                    }
                    if (local_addr != null && !local_addr.equals(data.getAddress())) {
                        addDiscoveryResponseToCaches(data.getAddress(), data.getLogicalName(), data.getPhysicalAddr());
                    }
                }
                // end copied block
            }
        } catch (Exception e) {
            log.error("Error unmarshalling ping data.", e);
        }
    }

    @Override
    protected void write(final List<PingData> list, final String clustername) {
        if (list == null || clustername == null) {
            return;
        }

        String filename = addressToFilename(clustername, local_addr);
        ByteArrayOutputStream out = new ByteArrayOutputStream(STREAM_BUFFER_SIZE);

        try {
            write(list, out);
            byte[] data = out.toByteArray();

            // Upload the file
            CloudBlockBlob blob = containerReference.getBlockBlobReference(filename);
            blob.upload(new ByteArrayInputStream(data), data.length);

        } catch (Exception ex) {
            log.error("Error marshalling and uploading ping data.", ex);
        }

    }

    @Override
    protected void remove(final String clustername, final Address addr) {
        if (clustername == null || addr == null) {
            return;
        }

        String filename = addressToFilename(clustername, addr);

        try {
            CloudBlockBlob blob = containerReference.getBlockBlobReference(filename);
            boolean deleted = blob.deleteIfExists();

            if (deleted) {
                log.debug("Tried to delete file '%s' but it was already deleted.", filename);
            } else {
                log.trace("Deleted file '%s'.", filename);
            }

        } catch (Exception ex) {
            log.error("Error deleting files.", ex);
        }
    }

    @Override
    protected void removeAll(String clustername) {
        if (clustername == null) {
            return;
        }

        clustername = sanitize(clustername);

        Iterable<ListBlobItem> listBlobItems = containerReference.listBlobs(clustername);
        for (ListBlobItem blobItem : listBlobItems) {
            try {
                // If the item is a blob and not a virtual directory.
                if (blobItem instanceof CloudBlob) {
                    CloudBlob blob = (CloudBlob) blobItem;
                    boolean deleted = blob.deleteIfExists();
                    if (deleted) {
                        log.debug("Tried to delete file '%s' but it was already deleted.", blob.getName());
                    } else {
                        log.trace("Deleted file '%s'.", blob.getName());
                    }
                }
            } catch (Exception e) {
                log.error("Error deleting ping data for cluster '" + clustername + "'.", e);
            }
        }
    }

    /**
     * Converts cluster name and address into a filename.
     */
    protected static String addressToFilename(final String clustername, final Address address) {
        return sanitize(clustername) + "-" + addressToFilename(address);
    }

    /**
     * Sanitizes names replacing backslashes and forward slashes with a dash.
     */
    protected static String sanitize(final String name) {
        return name.replace('/', '-').replace('\\', '-');
    }

    /**
     * The following classes has been copied/moved from Microsoft Azure client library.
     *
     * Copyright Microsoft Corporation
     *
     * Licensed under the Apache License, Version 2.0 (the "License");
     * you may not use this file except in compliance with the License.
     * You may obtain a copy of the License at
     * http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */

    public static final class StreamMd5AndLength {

        private String streamMd5;


        private long streamLength;


        private long currentOperationByteCount;


        private MessageDigest intermediateMD5;


        public MessageDigest getDigest() {
            return this.intermediateMD5;
        }


        public long getLength() {
            return this.streamLength;
        }


        public long getCurrentOperationByteCount() {
            return this.currentOperationByteCount;
        }


        public String getMd5() {
            if (this.streamMd5 == null && this.intermediateMD5 != null) {
                this.streamMd5 = Base64.encode(this.intermediateMD5.digest());
            }

            return this.streamMd5;
        }


        public void setDigest(MessageDigest digest) {
            this.intermediateMD5 = digest;
        }


        public void setLength(final long length) {
            this.streamLength = length;
        }


        public void setCurrentOperationByteCount(final long currentOperationByteCount) {
            this.currentOperationByteCount = currentOperationByteCount;
        }


        public void setMd5(final String md5) {
            this.streamMd5 = md5;
        }
    }

    public static final class PathUtility {

        public static URI addToSingleUriQuery(final URI resourceURI, final HashMap<String, String[]> fieldCollection)
                throws URISyntaxException, StorageException {
            if (resourceURI == null) {
                return null;
            }

            final UriQueryBuilder outUri = new UriQueryBuilder();

            // Generate new queryString
            for (final Map.Entry<String, String[]> entry : fieldCollection.entrySet()) {
                for (final String val : entry.getValue()) {
                    outUri.add(entry.getKey(), val);
                }
            }

            return outUri.addToURI(resourceURI);
        }


        public static StorageUri addToQuery(final StorageUri resourceURI, final String queryString)
                throws URISyntaxException, StorageException {
            return new StorageUri(addToSingleUriQuery(resourceURI.getPrimaryUri(), parseQueryString(queryString)),
                    addToSingleUriQuery(resourceURI.getSecondaryUri(), parseQueryString(queryString)));
        }


        public static URI addToQuery(final URI resourceURI, final String queryString) throws URISyntaxException,
                StorageException {
            return addToSingleUriQuery(resourceURI, parseQueryString(queryString));
        }


        public static StorageUri appendPathToUri(final StorageUri uriList, final String relativeOrAbsoluteUri)
                throws URISyntaxException {
            return appendPathToUri(uriList, relativeOrAbsoluteUri, "/");
        }


        public static StorageUri appendPathToUri(final StorageUri uriList, final String relativeOrAbsoluteUri,
                final String separator) throws URISyntaxException {
            return new StorageUri(appendPathToSingleUri(uriList.getPrimaryUri(), relativeOrAbsoluteUri, separator),
                    appendPathToSingleUri(uriList.getSecondaryUri(), relativeOrAbsoluteUri, separator));
        }


        public static URI appendPathToSingleUri(final URI uri, final String relativeOrAbsoluteUri)
                throws URISyntaxException {
            return appendPathToSingleUri(uri, relativeOrAbsoluteUri, "/");
        }


        public static URI appendPathToSingleUri(final URI uri, final String relativeUri, final String separator)
                throws URISyntaxException {

            if (uri == null) {
                return null;
            }

            if (relativeUri == null || relativeUri.isEmpty()) {
                return uri;
            }

            if (uri.getPath().length() == 0 && relativeUri.startsWith(separator)) {
                return new URI(uri.getScheme(), uri.getAuthority(), relativeUri, uri.getRawQuery(), uri.getRawFragment());
            }

            final StringBuilder pathString = new StringBuilder(uri.getPath());
            if (uri.getPath().endsWith(separator)) {
                pathString.append(relativeUri);
            }
            else {
                pathString.append(separator);
                pathString.append(relativeUri);
            }

            return new URI(uri.getScheme(), uri.getAuthority(), pathString.toString(), uri.getQuery(), uri.getFragment());
        }


        public static String getBlobNameFromURI(final URI inURI, final boolean usePathStyleUris) throws URISyntaxException {
            return Utility.safeRelativize(new URI(getContainerURI(new StorageUri(inURI), usePathStyleUris).getPrimaryUri()
                    .toString().concat("/")), inURI);
        }


        public static String getCanonicalPathFromCredentials(final StorageCredentials credentials, final String absolutePath) {
            final String account = credentials.getAccountName();

            if (account == null) {
                final String errorMessage = SR.CANNOT_CREATE_SAS_FOR_GIVEN_CREDENTIALS;
                throw new IllegalArgumentException(errorMessage);
            }
            final StringBuilder builder = new StringBuilder("/");
            builder.append(account);
            builder.append(absolutePath);
            return builder.toString();
        }


        public static String getContainerNameFromUri(final URI resourceAddress, final boolean usePathStyleUris) {
            return getResourceNameFromUri(resourceAddress, usePathStyleUris,
                    String.format("Invalid blob address '%s', missing container information", resourceAddress));
        }


        public static String getFileNameFromURI(final URI inURI, final boolean usePathStyleUris) {
            final String[] pathSegments = inURI.getRawPath().split("/");

            final int shareIndex = usePathStyleUris ? 2 : 1;

            if (pathSegments.length - 1 < shareIndex) {
                throw new IllegalArgumentException(String.format("Invalid file address '%s'.", inURI));
            }
            else if (pathSegments.length - 1 == shareIndex) {
                return "";
            }
            else {
                return pathSegments[pathSegments.length - 1];
            }
        }


        public static String getShareNameFromUri(final URI resourceAddress, final boolean usePathStyleUris) {
            return getResourceNameFromUri(resourceAddress, usePathStyleUris,
                    String.format("Invalid file address '%s', missing share information", resourceAddress));
        }


        public static String getTableNameFromUri(final URI resourceAddress, final boolean usePathStyleUris) {
            return getResourceNameFromUri(resourceAddress, usePathStyleUris,
                    String.format("Invalid table address '%s', missing table information", resourceAddress));
        }


        private static String getResourceNameFromUri(final URI resourceAddress, final boolean usePathStyleUris,
                final String error) {
            Utility.assertNotNull("resourceAddress", resourceAddress);

            final String[] pathSegments = resourceAddress.getRawPath().split("/");

            final int expectedPartsLength = usePathStyleUris ? 3 : 2;

            if (pathSegments.length < expectedPartsLength) {
                throw new IllegalArgumentException(error);
            }

            final String resourceName = usePathStyleUris ? pathSegments[2] : pathSegments[1];

            return Utility.trimEnd(resourceName, '/');
        }


        public static StorageUri getContainerURI(final StorageUri blobAddress, final boolean usePathStyleUris)
                throws URISyntaxException {
            final String containerName = getContainerNameFromUri(blobAddress.getPrimaryUri(), usePathStyleUris);

            final StorageUri containerUri = appendPathToUri(getServiceClientBaseAddress(blobAddress, usePathStyleUris),
                    containerName);
            return containerUri;
        }


        public static StorageUri getShareURI(final StorageUri fileAddress, final boolean usePathStyleUris)
                throws URISyntaxException {
            final String shareName = getShareNameFromUri(fileAddress.getPrimaryUri(), usePathStyleUris);

            final StorageUri shareUri = appendPathToUri(getServiceClientBaseAddress(fileAddress, usePathStyleUris),
                    shareName);
            return shareUri;
        }


        public static String getQueueNameFromUri(final URI resourceAddress, final boolean usePathStyleUris) {
            return getResourceNameFromUri(resourceAddress, usePathStyleUris,
                    String.format("Invalid queue URI '%s'.", resourceAddress));
        }


        public static String getServiceClientBaseAddress(final URI address, final boolean usePathStyleUris)
                throws URISyntaxException {
            if (address == null) {
                return null;
            }

            if (usePathStyleUris) {
                final String[] pathSegments = address.getRawPath().split("/");

                if (pathSegments.length < 2) {
                    final String error = String.format(SR.PATH_STYLE_URI_MISSING_ACCOUNT_INFORMATION);
                    throw new IllegalArgumentException(error);
                }

                final StringBuilder completeAddress = new StringBuilder(new URI(address.getScheme(),
                        address.getAuthority(), null, null, null).toString());
                completeAddress.append("/");
                completeAddress.append(Utility.trimEnd(pathSegments[1], '/'));

                return completeAddress.toString();
            }
            else {
                return new URI(address.getScheme(), address.getAuthority(), null, null, null).toString();
            }
        }


        public static StorageUri getServiceClientBaseAddress(final StorageUri addressUri, final boolean usePathStyleUris)
                throws URISyntaxException {
            return new StorageUri(new URI(getServiceClientBaseAddress(addressUri.getPrimaryUri(), usePathStyleUris)),
                    addressUri.getSecondaryUri() != null ? new URI(getServiceClientBaseAddress(
                            addressUri.getSecondaryUri(), usePathStyleUris)) : null);
        }


        public static HashMap<String, String[]> parseQueryString(String parseString) throws StorageException {
            final HashMap<String, String[]> retVals = new HashMap<String, String[]>();
            if (Utility.isNullOrEmpty(parseString)) {
                return retVals;
            }

            // 1. Remove ? if present
            final int queryDex = parseString.indexOf("?");
            if (queryDex >= 0 && parseString.length() > 0) {
                parseString = parseString.substring(queryDex + 1);
            }

            // 2. split name value pairs by splitting on the 'c&' character
            final String[] valuePairs = parseString.contains("&") ? parseString.split("&") : parseString.split(";");

            // 3. for each field value pair parse into appropriate map entries
            for (int m = 0; m < valuePairs.length; m++) {
                final int equalDex = valuePairs[m].indexOf("=");

                if (equalDex < 0 || equalDex == valuePairs[m].length() - 1) {
                    continue;
                }

                String key = valuePairs[m].substring(0, equalDex);
                String value = valuePairs[m].substring(equalDex + 1);

                key = Utility.safeDecode(key);
                value = Utility.safeDecode(value);

                // 3.1 add to map
                String[] values = retVals.get(key);

                if (values == null) {
                    values = new String[] { value };
                    if (!value.equals(Constants.EMPTY_STRING)) {
                        retVals.put(key, values);
                    }
                }
                else if (!value.equals(Constants.EMPTY_STRING)) {
                    final String[] newValues = new String[values.length + 1];
                    for (int j = 0; j < values.length; j++) {
                        newValues[j] = values[j];
                    }

                    newValues[newValues.length] = value;
                }
            }

            return retVals;
        }


        public static URI stripSingleURIQueryAndFragment(final URI inUri) throws StorageException {
            if (inUri == null) {
                return null;
            }
            try {
                return new URI(inUri.getScheme(), inUri.getAuthority(), inUri.getPath(), null, null);
            }
            catch (final URISyntaxException e) {
                throw Utility.generateNewUnexpectedStorageException(e);
            }
        }


        public static StorageUri stripURIQueryAndFragment(final StorageUri inUri) throws StorageException {
            return new StorageUri(stripSingleURIQueryAndFragment(inUri.getPrimaryUri()),
                    stripSingleURIQueryAndFragment(inUri.getSecondaryUri()));
        }


        private PathUtility() {
            // No op
        }
    }

    public static class NetworkInputStream extends InputStream {

        private final long expectedLength;

        private final InputStream inputStream;

        private long bytesRead = 0;


        public NetworkInputStream(InputStream stream, long expectedLength) {
            this.inputStream = stream;
            this.expectedLength = expectedLength;
        }

        @Override
        public int read() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int count = this.inputStream.read(b, off, len);
            if (count > -1) {
                this.bytesRead += count;
            }
            else {
                if (this.bytesRead != this.expectedLength) {
                    throw new IOException(SR.CONTENT_LENGTH_MISMATCH);
                }
            }

            return count;
        }

        @Override
        public void close() throws IOException {
            this.inputStream.close();
        }
    }

    public static class SharedAccessSignatureHelper {

        public static UriQueryBuilder generateSharedAccessSignatureForBlob(final SharedAccessBlobPolicy policy,
                final SharedAccessBlobHeaders headers, final String groupPolicyIdentifier, final String resourceType,
                final String signature) throws StorageException {
            Utility.assertNotNullOrEmpty("resourceType", resourceType);

            return generateSharedAccessSignatureHelper(policy, null , null ,
                    null , null , groupPolicyIdentifier, resourceType,
                    null , signature, null , headers);
        }


    //    public static UriQueryBuilder generateSharedAccessSignatureForTable(final SharedAccessTablePolicy policy,
    //            final String startPartitionKey, final String startRowKey, final String endPartitionKey,
    //            final String endRowKey, final String accessPolicyIdentifier, final String tableName,
    //            final String signature, final String accountKeyName) throws StorageException {
    //        Utility.assertNotNull("tableName", tableName);
    //        return generateSharedAccessSignatureHelper(policy, startPartitionKey, startRowKey, endPartitionKey, endRowKey,
    //                accessPolicyIdentifier, null , tableName, signature, accountKeyName, null );
    //    }


        public static String generateSharedAccessSignatureHashForBlob(final SharedAccessBlobPolicy policy,
                final SharedAccessBlobHeaders headers, final String accessPolicyIdentifier, final String resourceName,
                final ServiceClient client, final OperationContext opContext) throws InvalidKeyException, StorageException {
            return generateSharedAccessSignatureHashForBlob(policy, resourceName, accessPolicyIdentifier, client,
                    opContext, headers);

        }


        public static StorageCredentialsSharedAccessSignature parseQuery(final HashMap<String, String[]> queryParams)
                throws StorageException {
            String signature = null;
            String signedStart = null;
            String signedExpiry = null;
            String signedResource = null;
            String signedPermissions = null;
            String signedIdentifier = null;
            String signedVersion = null;
            String cacheControl = null;
            String contentType = null;
            String contentEncoding = null;
            String contentLanguage = null;
            String contentDisposition = null;
            String tableName = null;
            String startPk = null;
            String startRk = null;
            String endPk = null;
            String endRk = null;

            boolean sasParameterFound = false;

            StorageCredentialsSharedAccessSignature credentials = null;

            for (final Map.Entry<String, String[]> entry : queryParams.entrySet()) {
                final String lowerKey = entry.getKey().toLowerCase(Utility.LOCALE_US);

                if (lowerKey.equals(Constants.QueryConstants.SIGNED_START)) {
                    signedStart = entry.getValue()[0];
                    sasParameterFound = true;
                }
                else if (lowerKey.equals(Constants.QueryConstants.SIGNED_EXPIRY)) {
                    signedExpiry = entry.getValue()[0];
                    sasParameterFound = true;
                }
                else if (lowerKey.equals(Constants.QueryConstants.SIGNED_PERMISSIONS)) {
                    signedPermissions = entry.getValue()[0];
                    sasParameterFound = true;
                }
                else if (lowerKey.equals(Constants.QueryConstants.SIGNED_RESOURCE)) {
                    signedResource = entry.getValue()[0];
                    sasParameterFound = true;
                }
                else if (lowerKey.equals(Constants.QueryConstants.SIGNED_IDENTIFIER)) {
                    signedIdentifier = entry.getValue()[0];
                    sasParameterFound = true;
                }
                else if (lowerKey.equals(Constants.QueryConstants.SIGNATURE)) {
                    signature = entry.getValue()[0];
                    sasParameterFound = true;
                }
                else if (lowerKey.equals(Constants.QueryConstants.SIGNED_VERSION)) {
                    signedVersion = entry.getValue()[0];
                    sasParameterFound = true;
                }
                else if (lowerKey.equals(Constants.QueryConstants.CACHE_CONTROL)) {
                    cacheControl = entry.getValue()[0];
                    sasParameterFound = true;
                }
                else if (lowerKey.equals(Constants.QueryConstants.CONTENT_TYPE)) {
                    contentType = entry.getValue()[0];
                    sasParameterFound = true;
                }
                else if (lowerKey.equals(Constants.QueryConstants.CONTENT_ENCODING)) {
                    contentEncoding = entry.getValue()[0];
                    sasParameterFound = true;
                }
                else if (lowerKey.equals(Constants.QueryConstants.CONTENT_LANGUAGE)) {
                    contentLanguage = entry.getValue()[0];
                    sasParameterFound = true;
                }
                else if (lowerKey.equals(Constants.QueryConstants.CONTENT_DISPOSITION)) {
                    contentDisposition = entry.getValue()[0];
                    sasParameterFound = true;
                }
                else if (lowerKey.equals(Constants.QueryConstants.SAS_TABLE_NAME)) {
                    tableName = entry.getValue()[0];
                    sasParameterFound = true;
                }
                else if (lowerKey.equals(Constants.QueryConstants.START_PARTITION_KEY)) {
                    startPk = entry.getValue()[0];
                    sasParameterFound = true;
                }
                else if (lowerKey.equals(Constants.QueryConstants.START_ROW_KEY)) {
                    startRk = entry.getValue()[0];
                    sasParameterFound = true;
                }
                else if (lowerKey.equals(Constants.QueryConstants.END_PARTITION_KEY)) {
                    endPk = entry.getValue()[0];
                    sasParameterFound = true;
                }
                else if (lowerKey.equals(Constants.QueryConstants.END_ROW_KEY)) {
                    endRk = entry.getValue()[0];
                    sasParameterFound = true;
                }
            }

            if (sasParameterFound) {
                if (signature == null) {
                    final String errorMessage = SR.MISSING_MANDATORY_PARAMETER_FOR_SAS;
                    throw new IllegalArgumentException(errorMessage);
                }

                final UriQueryBuilder builder = new UriQueryBuilder();

                addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_START, signedStart);
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_EXPIRY, signedExpiry);
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_PERMISSIONS, signedPermissions);
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_RESOURCE, signedResource);
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_IDENTIFIER, signedIdentifier);
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_VERSION, signedVersion);
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNATURE, signature);

                addIfNotNullOrEmpty(builder, Constants.QueryConstants.CACHE_CONTROL, cacheControl);
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.CONTENT_TYPE, contentType);
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.CONTENT_ENCODING, contentEncoding);
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.CONTENT_LANGUAGE, contentLanguage);
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.CONTENT_DISPOSITION, contentDisposition);

                addIfNotNullOrEmpty(builder, Constants.QueryConstants.SAS_TABLE_NAME, tableName);
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.START_PARTITION_KEY, startPk);
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.START_ROW_KEY, startRk);
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.END_PARTITION_KEY, endPk);
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.END_ROW_KEY, endRk);

                final String token = builder.toString();
                credentials = new StorageCredentialsSharedAccessSignature(token);
            }

            return credentials;
        }


        private static void addIfNotNullOrEmpty(UriQueryBuilder builder, String name, String val) throws StorageException {
            if (!Utility.isNullOrEmpty(val)) {
                builder.add(name, val);
            }
        }


        private static UriQueryBuilder generateSharedAccessSignatureHelper(final SharedAccessPolicy policy,
                final String startPartitionKey, final String startRowKey, final String endPartitionKey,
                final String endRowKey, final String accessPolicyIdentifier, final String resourceType,
                final String tableName, final String signature, final String accountKeyName,
                final SharedAccessBlobHeaders headers) throws StorageException {
            Utility.assertNotNull("signature", signature);

            String permissions = null;
            Date startTime = null;
            Date expiryTime = null;

            if (policy != null) {
                permissions = policy.permissionsToString();
                startTime = policy.getSharedAccessStartTime();
                expiryTime = policy.getSharedAccessExpiryTime();
            }

            final UriQueryBuilder builder = new UriQueryBuilder();

            builder.add(Constants.QueryConstants.SIGNED_VERSION, Constants.HeaderConstants.TARGET_STORAGE_VERSION);

            addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_PERMISSIONS, permissions);

            final String startString = Utility.getUTCTimeOrEmpty(startTime);
            addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_START, startString);

            final String stopString = Utility.getUTCTimeOrEmpty(expiryTime);
            addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_EXPIRY, stopString);

            addIfNotNullOrEmpty(builder, Constants.QueryConstants.START_PARTITION_KEY, startPartitionKey);
            addIfNotNullOrEmpty(builder, Constants.QueryConstants.START_ROW_KEY, startRowKey);
            addIfNotNullOrEmpty(builder, Constants.QueryConstants.END_PARTITION_KEY, endPartitionKey);
            addIfNotNullOrEmpty(builder, Constants.QueryConstants.END_ROW_KEY, endRowKey);

            addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_IDENTIFIER, accessPolicyIdentifier);
            addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_RESOURCE, resourceType);

            addIfNotNullOrEmpty(builder, Constants.QueryConstants.SAS_TABLE_NAME, tableName);

            if (headers != null) {
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.CACHE_CONTROL, headers.getCacheControl());
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.CONTENT_TYPE, headers.getContentType());
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.CONTENT_ENCODING, headers.getContentEncoding());
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.CONTENT_LANGUAGE, headers.getContentLanguage());
                addIfNotNullOrEmpty(builder, Constants.QueryConstants.CONTENT_DISPOSITION, headers.getContentDisposition());
            }

            addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNATURE, signature);
            addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_KEY, accountKeyName);

            return builder;
        }


        private static String generateSharedAccessSignatureHashForBlob(final SharedAccessPolicy policy,
                final String resourceName, final String accessPolicyIdentifier, final ServiceClient client,
                final OperationContext opContext, final SharedAccessBlobHeaders headers) throws InvalidKeyException,
                StorageException {
            Utility.assertNotNullOrEmpty("resourceName", resourceName);
            Utility.assertNotNull("client", client);

            String permissions = null;
            Date startTime = null;
            Date expiryTime = null;

            if (policy != null) {
                permissions = policy.permissionsToString();
                startTime = policy.getSharedAccessStartTime();
                expiryTime = policy.getSharedAccessExpiryTime();
            }

            String cacheControl = null;
            String contentDisposition = null;
            String contentEncoding = null;
            String contentLanguage = null;
            String contentType = null;

            if (headers != null) {
                cacheControl = headers.getCacheControl();
                contentDisposition = headers.getContentDisposition();
                contentEncoding = headers.getContentEncoding();
                contentLanguage = headers.getContentLanguage();
                contentType = headers.getContentType();
            }

            String stringToSign = String.format("%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s",
                    permissions == null ? Constants.EMPTY_STRING : permissions, Utility.getUTCTimeOrEmpty(startTime),
                    Utility.getUTCTimeOrEmpty(expiryTime), resourceName,
                    accessPolicyIdentifier == null ? Constants.EMPTY_STRING : accessPolicyIdentifier,
                    Constants.HeaderConstants.TARGET_STORAGE_VERSION, cacheControl == null ? Constants.EMPTY_STRING
                            : cacheControl, contentDisposition == null ? Constants.EMPTY_STRING : contentDisposition,
                    contentEncoding == null ? Constants.EMPTY_STRING : contentEncoding,
                    contentLanguage == null ? Constants.EMPTY_STRING : contentLanguage,
                    contentType == null ? Constants.EMPTY_STRING : contentType);

            stringToSign = Utility.safeDecode(stringToSign);
            final String signature = StorageCredentialsHelper.computeHmac256(client.getCredentials(), stringToSign,
                    opContext);

            // add logging
            return signature;
        }


        private SharedAccessSignatureHelper() {
            // No op
        }
    }

    public static class SegmentedStorageRequest {


        private ResultContinuation token = null;


        public final ResultContinuation getToken() {
            return this.token;
        }


        public final void setToken(final ResultContinuation token) {
            this.token = token;
        }
    }

    public static final class BaseRequest {

        private static final String METADATA = "metadata";

        private static final String SERVICE = "service";

        private static final String STATS = "stats";

        private static final String TIMEOUT = "timeout";


        private static String userAgent;


        public static void addMetadata(final HttpURLConnection request, final HashMap<String, String> metadata,
                final OperationContext opContext) {
            if (metadata != null) {
                for (final Map.Entry<String, String> entry : metadata.entrySet()) {
                    addMetadata(request, entry.getKey(), entry.getValue(), opContext);
                }
            }
        }


        private static void addMetadata(final HttpURLConnection request, final String name, final String value,
                final OperationContext opContext) {
            if (Utility.isNullOrEmptyOrWhitespace(name)) {
                throw new IllegalArgumentException(SR.METADATA_KEY_INVALID);
            }
            else if (Utility.isNullOrEmptyOrWhitespace(value)) {
                throw new IllegalArgumentException(SR.METADATA_VALUE_INVALID);
            }

            request.setRequestProperty(Constants.HeaderConstants.PREFIX_FOR_STORAGE_METADATA + name, value);
        }


        public static void addOptionalHeader(final HttpURLConnection request, final String name, final String value) {
            if (value != null && !value.equals(Constants.EMPTY_STRING)) {
                request.setRequestProperty(name, value);
            }
        }


        public static HttpURLConnection create(final URI uri, final RequestOptions options, UriQueryBuilder builder,
                final OperationContext opContext) throws IOException, URISyntaxException, StorageException {
            if (builder == null) {
                builder = new UriQueryBuilder();
            }

            final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);
            retConnection.setFixedLengthStreamingMode(0);
            retConnection.setDoOutput(true);
            retConnection.setRequestMethod(Constants.HTTP_PUT);

            return retConnection;
        }


        public static HttpURLConnection createURLConnection(final URI uri, final RequestOptions options,
                UriQueryBuilder builder, final OperationContext opContext) throws IOException, URISyntaxException,
                StorageException {
            if (builder == null) {
                builder = new UriQueryBuilder();
            }

            final URL resourceUrl = builder.addToURI(uri).toURL();

            final HttpURLConnection retConnection = (HttpURLConnection) resourceUrl.openConnection();

            if (options.getTimeoutIntervalInMs() != null && options.getTimeoutIntervalInMs() != 0) {
                builder.add(TIMEOUT, String.valueOf(options.getTimeoutIntervalInMs() / 1000));
            }

            // Note: ReadTimeout must be explicitly set to avoid a bug in JDK 6.
            // In certain cases, this bug causes an immediate read timeout exception to be thrown even if ReadTimeout is not set.
            retConnection.setReadTimeout(Utility.getRemainingTimeout(options.getOperationExpiryTimeInMs(), options.getTimeoutIntervalInMs()));

            // Note : accept behavior, java by default sends Accept behavior as text/html, image/gif, image/jpeg, *; q=.2, * / *; q=.2
            retConnection.setRequestProperty(Constants.HeaderConstants.ACCEPT, Constants.HeaderConstants.XML_TYPE);
            retConnection.setRequestProperty(Constants.HeaderConstants.ACCEPT_CHARSET, Constants.UTF8_CHARSET);

            // Note : Content-Type behavior, java by default sends Content-type behavior as application/x-www-form-urlencoded for posts.
            retConnection.setRequestProperty(Constants.HeaderConstants.CONTENT_TYPE, Constants.EMPTY_STRING);

            retConnection.setRequestProperty(Constants.HeaderConstants.STORAGE_VERSION_HEADER,
                    Constants.HeaderConstants.TARGET_STORAGE_VERSION);
            retConnection.setRequestProperty(Constants.HeaderConstants.USER_AGENT, getUserAgent());
            retConnection.setRequestProperty(Constants.HeaderConstants.CLIENT_REQUEST_ID_HEADER,
                    opContext.getClientRequestID());

            return retConnection;
        }


        public static HttpURLConnection delete(final URI uri, final RequestOptions options, UriQueryBuilder builder,
                final OperationContext opContext) throws IOException, URISyntaxException, StorageException {
            if (builder == null) {
                builder = new UriQueryBuilder();
            }

            final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);
            retConnection.setRequestMethod(Constants.HTTP_DELETE);

            return retConnection;
        }


        public static UriQueryBuilder getListUriQueryBuilder(final ListingContext listingContext) throws StorageException {
            final UriQueryBuilder builder = new UriQueryBuilder();
            builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.LIST);

            if (listingContext != null) {
                if (!Utility.isNullOrEmpty(listingContext.getPrefix())) {
                    builder.add(Constants.QueryConstants.PREFIX, listingContext.getPrefix());
                }

                if (!Utility.isNullOrEmpty(listingContext.getMarker())) {
                    builder.add(Constants.QueryConstants.MARKER, listingContext.getMarker());
                }

                if (listingContext.getMaxResults() != null && listingContext.getMaxResults() > 0) {
                    builder.add(Constants.QueryConstants.MAX_RESULTS, listingContext.getMaxResults().toString());
                }
            }

            return builder;
        }


        public static HttpURLConnection getProperties(final URI uri, final RequestOptions options, UriQueryBuilder builder,
                final OperationContext opContext) throws IOException, URISyntaxException, StorageException {
            if (builder == null) {
                builder = new UriQueryBuilder();
            }

            final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);
            retConnection.setRequestMethod(Constants.HTTP_HEAD);

            return retConnection;
        }


        public static HttpURLConnection getServiceProperties(final URI uri, final RequestOptions options,
                UriQueryBuilder builder, final OperationContext opContext) throws IOException, URISyntaxException,
                StorageException {
            if (builder == null) {
                builder = new UriQueryBuilder();
            }

            builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.PROPERTIES);
            builder.add(Constants.QueryConstants.RESOURCETYPE, SERVICE);

            final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);
            retConnection.setRequestMethod(Constants.HTTP_GET);

            return retConnection;
        }


        public static HttpURLConnection getServiceStats(final URI uri, final RequestOptions options,
                UriQueryBuilder builder, final OperationContext opContext) throws IOException, URISyntaxException,
                StorageException {
            if (builder == null) {
                builder = new UriQueryBuilder();
            }

            builder.add(Constants.QueryConstants.COMPONENT, STATS);
            builder.add(Constants.QueryConstants.RESOURCETYPE, SERVICE);

            final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);
            retConnection.setRequestMethod("GET");

            return retConnection;
        }


        public static String getUserAgent() {
            if (userAgent == null) {
                String userAgentComment = String.format(Utility.LOCALE_US, "(JavaJRE %s; %s %s)",
                        System.getProperty("java.version"), System.getProperty("os.name").replaceAll(" ", ""),
                        System.getProperty("os.version"));
                userAgent = String.format("%s/%s %s", Constants.HeaderConstants.USER_AGENT_PREFIX,
                        Constants.HeaderConstants.USER_AGENT_VERSION, userAgentComment);
            }

            return userAgent;
        }


        public static HttpURLConnection setMetadata(final URI uri, final RequestOptions options, UriQueryBuilder builder,
                final OperationContext opContext) throws IOException, URISyntaxException, StorageException {

            if (builder == null) {
                builder = new UriQueryBuilder();
            }

            builder.add(Constants.QueryConstants.COMPONENT, METADATA);
            final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);

            retConnection.setFixedLengthStreamingMode(0);
            retConnection.setDoOutput(true);
            retConnection.setRequestMethod(Constants.HTTP_PUT);

            return retConnection;
        }


        public static HttpURLConnection setServiceProperties(final URI uri, final RequestOptions options,
                UriQueryBuilder builder, final OperationContext opContext) throws IOException, URISyntaxException,
                StorageException {
            if (builder == null) {
                builder = new UriQueryBuilder();
            }

            builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.PROPERTIES);
            builder.add(Constants.QueryConstants.RESOURCETYPE, SERVICE);

            final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);

            retConnection.setDoOutput(true);
            retConnection.setRequestMethod(Constants.HTTP_PUT);

            return retConnection;
        }


        public static void signRequestForBlobAndQueue(final HttpURLConnection request, final Credentials credentials,
                final Long contentLength, final OperationContext opContext) throws InvalidKeyException, StorageException {
            request.setRequestProperty(Constants.HeaderConstants.DATE, Utility.getGMTTime());
            final Canonicalizer canonicalizer = CanonicalizerFactory.getBlobQueueFullCanonicalizer(request);

            final String stringToSign = canonicalizer.canonicalize(request, credentials.getAccountName(), contentLength);

            final String computedBase64Signature = StorageKey.computeMacSha256(credentials.getKey(), stringToSign);

            // V2 add logging
            // System.out.println(String.format("Signing %s\r\n%s\r\n", stringToSign, computedBase64Signature));
            request.setRequestProperty(Constants.HeaderConstants.AUTHORIZATION,
                    String.format("%s %s:%s", "SharedKey", credentials.getAccountName(), computedBase64Signature));
        }


        @Deprecated
        public static void signRequestForBlobAndQueueSharedKeyLite(final HttpURLConnection request,
                final Credentials credentials, final Long contentLength, final OperationContext opContext)
                throws InvalidKeyException, StorageException {
            request.setRequestProperty(Constants.HeaderConstants.DATE, Utility.getGMTTime());

            final Canonicalizer canonicalizer = CanonicalizerFactory.getBlobQueueLiteCanonicalizer(request);

            final String stringToSign = canonicalizer.canonicalize(request, credentials.getAccountName(), contentLength);

            final String computedBase64Signature = StorageKey.computeMacSha256(credentials.getKey(), stringToSign);

            // VNext add logging
            // System.out.println(String.format("Signing %s\r\n%s\r\n",
            // stringToSign, computedBase64Signature));
            request.setRequestProperty(Constants.HeaderConstants.AUTHORIZATION,
                    String.format("%s %s:%s", "SharedKeyLite", credentials.getAccountName(), computedBase64Signature));
        }


        public static void signRequestForTableSharedKey(final HttpURLConnection request, final Credentials credentials,
                final Long contentLength, final OperationContext opContext) throws InvalidKeyException, StorageException {
            request.setRequestProperty(Constants.HeaderConstants.DATE, Utility.getGMTTime());

            final Canonicalizer canonicalizer = CanonicalizerFactory.getTableFullCanonicalizer(request);

            final String stringToSign = canonicalizer.canonicalize(request, credentials.getAccountName(), contentLength);

            final String computedBase64Signature = StorageKey.computeMacSha256(credentials.getKey(), stringToSign);

            request.setRequestProperty(Constants.HeaderConstants.AUTHORIZATION,
                    String.format("%s %s:%s", "SharedKey", credentials.getAccountName(), computedBase64Signature));
        }


        @Deprecated
        public static void signRequestForTableSharedKeyLite(final HttpURLConnection request, final Credentials credentials,
                final Long contentLength, final OperationContext opContext) throws InvalidKeyException, StorageException {
            request.setRequestProperty(Constants.HeaderConstants.DATE, Utility.getGMTTime());

            final Canonicalizer canonicalizer = CanonicalizerFactory.getTableLiteCanonicalizer(request);

            final String stringToSign = canonicalizer.canonicalize(request, credentials.getAccountName(), contentLength);

            final String computedBase64Signature = StorageKey.computeMacSha256(credentials.getKey(), stringToSign);

            request.setRequestProperty(Constants.HeaderConstants.AUTHORIZATION,
                    String.format("%s %s:%s", "SharedKeyLite", credentials.getAccountName(), computedBase64Signature));
        }


        private BaseRequest() {
            // No op
        }
    }

    public static enum RequestLocationMode {
        PRIMARY_ONLY, SECONDARY_ONLY, PRIMARY_OR_SECONDARY;
    }

    static final class BlobQueueLiteCanonicalizer extends Canonicalizer {


        @Override
        protected String canonicalize(final HttpURLConnection conn, final String accountName, final Long contentLength) throws StorageException {
            if (contentLength < -1) {
                throw new InvalidParameterException(SR.INVALID_CONTENT_LENGTH);
            }

            return canonicalizeHttpRequestLite(conn.getURL(), accountName, conn.getRequestMethod(),
                    Utility.getStandardHeaderValue(conn, Constants.HeaderConstants.CONTENT_TYPE), contentLength, null,
                    conn);
        }
    }

    public static final class UriQueryBuilder {

        private final HashMap<String, ArrayList<String>> parameters = new HashMap<String, ArrayList<String>>();


        public void add(final String name, final String value) throws StorageException {
            if (Utility.isNullOrEmpty(name)) {
                throw new IllegalArgumentException(SR.QUERY_PARAMETER_NULL_OR_EMPTY);
            }

            this.insertKeyValue(name, value);
        }


        public URI addToURI(final URI uri) throws URISyntaxException, StorageException {
            final String origRawQuery = uri.getRawQuery();
            final String rawFragment = uri.getRawFragment();
            final String uriString = uri.resolve(uri).toASCIIString();

            final HashMap<String, String[]> origQueryMap = PathUtility.parseQueryString(origRawQuery);

            // Try/Insert original queries to map

            for (final Map.Entry<String, String[]> entry : origQueryMap.entrySet()) {
                for (final String val : entry.getValue()) {
                    this.insertKeyValue(entry.getKey(), val);
                }
            }

            final StringBuilder retBuilder = new StringBuilder();

            // has a fragment
            if (Utility.isNullOrEmpty(origRawQuery) && !Utility.isNullOrEmpty(rawFragment)) {
                final int bangDex = uriString.indexOf('#');
                retBuilder.append(uriString.substring(0, bangDex));
            }
            else if (!Utility.isNullOrEmpty(origRawQuery)) {
                // has a query
                final int queryDex = uriString.indexOf('?');
                retBuilder.append(uriString.substring(0, queryDex));
            }
            else {
                // no fragment or query
                retBuilder.append(uriString);
                if (uri.getRawPath().length() <= 0) {
                    retBuilder.append("/");
                }
            }

            final String finalQuery = this.toString();

            if (finalQuery.length() > 0) {
                retBuilder.append("?");
                retBuilder.append(finalQuery);
            }

            if (!Utility.isNullOrEmpty(rawFragment)) {
                retBuilder.append("#");
                retBuilder.append(rawFragment);
            }

            return new URI(retBuilder.toString());
        }


        private void insertKeyValue(String key, String value) throws StorageException {
            if (value != null) {
                value = Utility.safeEncode(value);
            }
            if (!key.startsWith("$")) {
                key = Utility.safeEncode(key);
            }

            ArrayList<String> list = this.parameters.get(key);
            if (list == null) {
                list = new ArrayList<String>();
                list.add(value);
                this.parameters.put(key, list);
            }
            else {
                if (!list.contains(value)) {
                    list.add(value);
                }
            }
        }


        @Override
        public String toString() {
            final StringBuilder outString = new StringBuilder();
            Boolean isFirstPair = true;

            for (final String key : this.parameters.keySet()) {
                if (this.parameters.get(key) != null) {
                    for (final String val : this.parameters.get(key)) {
                        if (isFirstPair) {
                            isFirstPair = false;
                        }
                        else {
                            outString.append("&");
                        }

                        outString.append(String.format("%s=%s", key, val));
                    }
                }
            }

            return outString.toString();
        }
    }

    public static class LogConstants {
        public static final String COMPLETE = "Operation completed.";
        public static final String DO_NOT_RETRY_POLICY = "Retry policy did not allow for a retry. Failing. Error Message = '%s'.";
        public static final String DO_NOT_RETRY_TIMEOUT = "Operation cannot be retried because maximum execution timeout has been reached. Failing. Inner error Message = '%s'.";
        public static final String GET_RESPONSE = "Waiting for response.";
        public static final String INIT_LOCATION = "Starting operation with location '%s' per location mode '%s'.";
        public static final String NEXT_LOCATION = "The next location has been set to '%s', per location mode '%s'.";
        public static final String POST_PROCESS = "Processing response body.";
        public static final String POST_PROCESS_DONE = "Response body was parsed successfully.";
        public static final String PRE_PROCESS = "Processing response headers.";
        public static final String PRE_PROCESS_DONE = "Response headers were processed successfully.";
        public static final String RESPONSE_RECEIVED = "Response received. Status code = '%d', Request ID = '%s', Content-MD5 = '%s', ETag = '%s', Date = '%s'.";
        public static final String RETRY = "Retrying failed operation.";
        public static final String RETRY_CHECK = "Checking if the operation should be retried. Retry count = '%d', HTTP status code = '%d', Error Message = '%s'.";
        public static final String RETRY_DELAY = "Operation will be retried after '%d'ms.";
        public static final String RETRY_INFO = "The retry policy set the next location to '%s' and updated the location mode to '%s'.";
        public static final String RETRYABLE_EXCEPTION = "Retryable exception thrown. Class = '%s', Message = '%s'.";
        public static final String START_REQUEST = "Starting request to '%s' at '%s'.";
        public static final String STARTING = "Starting operation.";
        public static final String UNEXPECTED_RESULT_OR_EXCEPTION = "Operation did not return the expected result or returned an exception.";
        public static final String UNRETRYABLE_EXCEPTION = "Un-retryable exception thrown. Class = '%s', Message = '%s'.";
        public static final String UPLOAD = "Writing request data.";
        public static final String UPLOADDONE = "Request data was written successfully.";
    }

    abstract static class Canonicalizer {


        private static final int ExpectedBlobQueueCanonicalizedStringLength = 300;


        private static final int ExpectedBlobQueueLiteCanonicalizedStringLength = 250;


        private static final int ExpectedTableCanonicalizedStringLength = 200;


        private static void addCanonicalizedHeaders(final HttpURLConnection conn, final StringBuilder canonicalizedString) {
            // Look for header names that start with
            // HeaderNames.PrefixForStorageHeader
            // Then sort them in case-insensitive manner.

            final Map<String, List<String>> headers = conn.getRequestProperties();
            final ArrayList<String> httpStorageHeaderNameArray = new ArrayList<String>();

            for (final String key : headers.keySet()) {
                if (key.toLowerCase(Utility.LOCALE_US).startsWith(Constants.PREFIX_FOR_STORAGE_HEADER)) {
                    httpStorageHeaderNameArray.add(key.toLowerCase(Utility.LOCALE_US));
                }
            }

            Collections.sort(httpStorageHeaderNameArray);

            // Now go through each header's values in the sorted order and append
            // them to the canonicalized string.
            for (final String key : httpStorageHeaderNameArray) {
                final StringBuilder canonicalizedElement = new StringBuilder(key);
                String delimiter = ":";
                final ArrayList<String> values = getHeaderValues(headers, key);

                boolean appendCanonicalizedElement = false;
                // Go through values, unfold them, and then append them to the
                // canonicalized element string.
                for (final String value : values) {
                    if (!Utility.isNullOrEmpty(value)) {
                        appendCanonicalizedElement = true;
                    }

                    // Unfolding is simply removal of CRLF.
                    final String unfoldedValue = value.replace("\r\n", Constants.EMPTY_STRING);

                    // Append it to the canonicalized element string.
                    canonicalizedElement.append(delimiter);
                    canonicalizedElement.append(unfoldedValue);
                    delimiter = ",";
                }

                // Now, add this canonicalized element to the canonicalized header
                // string.
                if (appendCanonicalizedElement) {
                    appendCanonicalizedElement(canonicalizedString, canonicalizedElement.toString());
                }
            }
        }


        protected static void appendCanonicalizedElement(final StringBuilder builder, final String element) {
            builder.append("\n");
            builder.append(element);
        }


        protected static String canonicalizeHttpRequest(final URL address, final String accountName,
                final String method, final String contentType, final long contentLength, final String date,
                final HttpURLConnection conn) throws StorageException {

            // The first element should be the Method of the request.
            // I.e. GET, POST, PUT, or HEAD.
            final StringBuilder canonicalizedString = new StringBuilder(ExpectedBlobQueueCanonicalizedStringLength);
            canonicalizedString.append(conn.getRequestMethod());

            // The next elements are
            // If any element is missing it may be empty.
            appendCanonicalizedElement(canonicalizedString,
                    Utility.getStandardHeaderValue(conn, Constants.HeaderConstants.CONTENT_ENCODING));
            appendCanonicalizedElement(canonicalizedString,
                    Utility.getStandardHeaderValue(conn, Constants.HeaderConstants.CONTENT_LANGUAGE));
            appendCanonicalizedElement(canonicalizedString,
                    contentLength == -1 ? Constants.EMPTY_STRING : String.valueOf(contentLength));
            appendCanonicalizedElement(canonicalizedString,
                    Utility.getStandardHeaderValue(conn, Constants.HeaderConstants.CONTENT_MD5));
            appendCanonicalizedElement(canonicalizedString, contentType != null ? contentType : Constants.EMPTY_STRING);

            final String dateString = Utility.getStandardHeaderValue(conn, Constants.HeaderConstants.DATE);
            // If x-ms-date header exists, Date should be empty string
            appendCanonicalizedElement(canonicalizedString, dateString.equals(Constants.EMPTY_STRING) ? date
                    : Constants.EMPTY_STRING);

            appendCanonicalizedElement(canonicalizedString,
                    Utility.getStandardHeaderValue(conn, Constants.HeaderConstants.IF_MODIFIED_SINCE));
            appendCanonicalizedElement(canonicalizedString,
                    Utility.getStandardHeaderValue(conn, Constants.HeaderConstants.IF_MATCH));
            appendCanonicalizedElement(canonicalizedString,
                    Utility.getStandardHeaderValue(conn, Constants.HeaderConstants.IF_NONE_MATCH));
            appendCanonicalizedElement(canonicalizedString,
                    Utility.getStandardHeaderValue(conn, Constants.HeaderConstants.IF_UNMODIFIED_SINCE));
            appendCanonicalizedElement(canonicalizedString,
                    Utility.getStandardHeaderValue(conn, Constants.HeaderConstants.RANGE));

            addCanonicalizedHeaders(conn, canonicalizedString);

            appendCanonicalizedElement(canonicalizedString, getCanonicalizedResource(address, accountName));

            return canonicalizedString.toString();
        }


        protected static String canonicalizeHttpRequestLite(final URL address, final String accountName,
                final String method, final String contentType, final long contentLength, final String date,
                final HttpURLConnection conn) throws StorageException {
            // The first element should be the Method of the request.
            // I.e. GET, POST, PUT, or HEAD.
            //
            final StringBuilder canonicalizedString = new StringBuilder(ExpectedBlobQueueLiteCanonicalizedStringLength);
            canonicalizedString.append(conn.getRequestMethod());

            // The second element should be the MD5 value.
            // This is optional and may be empty.
            final String httpContentMD5Value = Utility.getStandardHeaderValue(conn, Constants.HeaderConstants.CONTENT_MD5);
            appendCanonicalizedElement(canonicalizedString, httpContentMD5Value);

            // The third element should be the content type.
            appendCanonicalizedElement(canonicalizedString, contentType);

            // The fourth element should be the request date.
            // See if there's an storage date header.
            // If there's one, then don't use the date header.

            final String dateString = Utility.getStandardHeaderValue(conn, Constants.HeaderConstants.DATE);
            // If x-ms-date header exists, Date should be empty string
            appendCanonicalizedElement(canonicalizedString, dateString.equals(Constants.EMPTY_STRING) ? date
                    : Constants.EMPTY_STRING);

            addCanonicalizedHeaders(conn, canonicalizedString);

            appendCanonicalizedElement(canonicalizedString, getCanonicalizedResourceLite(address, accountName));

            return canonicalizedString.toString();
        }


        protected static String canonicalizeTableHttpRequest(final URL address, final String accountName,
                final String method, final String contentType, final long contentLength, final String date,
                final HttpURLConnection conn) throws StorageException {
            // The first element should be the Method of the request.
            // I.e. GET, POST, PUT, or HEAD.
            final StringBuilder canonicalizedString = new StringBuilder(ExpectedTableCanonicalizedStringLength);
            canonicalizedString.append(conn.getRequestMethod());

            // The second element should be the MD5 value.
            // This is optional and may be empty.
            final String httpContentMD5Value = Utility.getStandardHeaderValue(conn, Constants.HeaderConstants.CONTENT_MD5);
            appendCanonicalizedElement(canonicalizedString, httpContentMD5Value);

            // The third element should be the content type.
            appendCanonicalizedElement(canonicalizedString, contentType);

            // The fourth element should be the request date.
            // See if there's an storage date header.
            // If there's one, then don't use the date header.

            final String dateString = Utility.getStandardHeaderValue(conn, Constants.HeaderConstants.DATE);
            // If x-ms-date header exists, Date should be that value.
            appendCanonicalizedElement(canonicalizedString, dateString.equals(Constants.EMPTY_STRING) ? date : dateString);

            appendCanonicalizedElement(canonicalizedString, getCanonicalizedResourceLite(address, accountName));

            return canonicalizedString.toString();
        }


        protected static String getCanonicalizedResource(final URL address, final String accountName)
                throws StorageException {
            // Resource path
            final StringBuilder resourcepath = new StringBuilder("/");
            resourcepath.append(accountName);

            // Note that AbsolutePath starts with a '/'.
            resourcepath.append(address.getPath());
            final StringBuilder canonicalizedResource = new StringBuilder(resourcepath.toString());

            // query parameters
            final Map<String, String[]> queryVariables = PathUtility.parseQueryString(address.getQuery());

            final Map<String, String> lowercasedKeyNameValue = new HashMap<String, String>();

            for (final Map.Entry<String, String[]> entry : queryVariables.entrySet()) {
                // sort the value and organize it as comma separated values
                final List<String> sortedValues = Arrays.asList(entry.getValue());
                Collections.sort(sortedValues);

                final StringBuilder stringValue = new StringBuilder();

                for (final String value : sortedValues) {
                    if (stringValue.length() > 0) {
                        stringValue.append(",");
                    }

                    stringValue.append(value);
                }

                // key turns out to be null for ?a&b&c&d
                lowercasedKeyNameValue.put(entry.getKey() == null ? null : entry.getKey().toLowerCase(Utility.LOCALE_US),
                        stringValue.toString());
            }

            final ArrayList<String> sortedKeys = new ArrayList<String>(lowercasedKeyNameValue.keySet());

            Collections.sort(sortedKeys);

            for (final String key : sortedKeys) {
                final StringBuilder queryParamString = new StringBuilder();

                queryParamString.append(key);
                queryParamString.append(":");
                queryParamString.append(lowercasedKeyNameValue.get(key));

                appendCanonicalizedElement(canonicalizedResource, queryParamString.toString());
            }

            return canonicalizedResource.toString();
        }


        protected static String getCanonicalizedResourceLite(final URL address, final String accountName)
                throws StorageException {
            // Resource path
            final StringBuilder resourcepath = new StringBuilder("/");
            resourcepath.append(accountName);

            // Note that AbsolutePath starts with a '/'.
            resourcepath.append(address.getPath());
            final StringBuilder canonicalizedResource = new StringBuilder(resourcepath.toString());

            // query parameters
            final Map<String, String[]> queryVariables = PathUtility.parseQueryString(address.getQuery());

            final String[] compVals = queryVariables.get("comp");

            if (compVals != null) {

                final List<String> sortedValues = Arrays.asList(compVals);
                Collections.sort(sortedValues);

                canonicalizedResource.append("?comp=");

                final StringBuilder stringValue = new StringBuilder();
                for (final String value : sortedValues) {
                    if (stringValue.length() > 0) {
                        stringValue.append(",");
                    }
                    stringValue.append(value);
                }

                canonicalizedResource.append(stringValue);
            }

            return canonicalizedResource.toString();
        }


        private static ArrayList<String> getHeaderValues(final Map<String, List<String>> headers, final String headerName) {

            final ArrayList<String> arrayOfValues = new ArrayList<String>();
            List<String> values = null;

            for (final Map.Entry<String, List<String>> entry : headers.entrySet()) {
                if (entry.getKey().toLowerCase(Utility.LOCALE_US).equals(headerName)) {
                    values = entry.getValue();
                    break;
                }
            }
            if (values != null) {
                for (final String value : values) {
                    // canonicalization formula requires the string to be left
                    // trimmed.
                    arrayOfValues.add(Utility.trimStart(value));
                }
            }
            return arrayOfValues;
        }


        protected abstract String canonicalize(HttpURLConnection conn, String accountName, Long contentLength)
                throws StorageException;
    }

    public static final class Utility {

        public static final TimeZone GMT_ZONE = TimeZone.getTimeZone("GMT");


        public static final TimeZone UTC_ZONE = TimeZone.getTimeZone("UTC");


        public static final Locale LOCALE_US = Locale.US;


        private static final String RFC1123_PATTERN = "EEE, dd MMM yyyy HH:mm:ss z";


        private static final String ISO8601_PATTERN_NO_SECONDS = "yyyy-MM-dd'T'HH:mm'Z'";


        private static final String ISO8601_PATTERN = "yyyy-MM-dd'T'HH:mm:ss'Z'";


        private static final String JAVA_ISO8601_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";


        private static final List<Integer> pathStylePorts = Arrays.asList(10000, 10001, 10002, 10003, 10004, 10100, 10101,
                10102, 10103, 10104, 11000, 11001, 11002, 11003, 11004, 11100, 11101, 11102, 11103, 11104);


        private static final SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();


        private static final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();


        private static final String MAX_PRECISION_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS";


        private static final int MAX_PRECISION_DATESTRING_LENGTH = MAX_PRECISION_PATTERN.replaceAll("'", "").length();


        public static StreamMd5AndLength analyzeStream(final InputStream sourceStream, long writeLength,
                long abandonLength, final boolean rewindSourceStream, final boolean calculateMD5) throws IOException,
                StorageException {
            if (abandonLength < 0) {
                abandonLength = Long.MAX_VALUE;
            }

            if (rewindSourceStream) {
                if (!sourceStream.markSupported()) {
                    throw new IllegalArgumentException(SR.INPUT_STREAM_SHOULD_BE_MARKABLE);
                }

                sourceStream.mark(Constants.MAX_MARK_LENGTH);
            }

            MessageDigest digest = null;
            if (calculateMD5) {
                try {
                    digest = MessageDigest.getInstance("MD5");
                }
                catch (final NoSuchAlgorithmException e) {
                    // This wont happen, throw fatal.
                    throw Utility.generateNewUnexpectedStorageException(e);
                }
            }

            if (writeLength < 0) {
                writeLength = Long.MAX_VALUE;
            }

            final StreamMd5AndLength retVal = new StreamMd5AndLength();
            int count = -1;
            final byte[] retrievedBuff = new byte[Constants.BUFFER_COPY_LENGTH];

            int nextCopy = (int) Math.min(retrievedBuff.length, writeLength - retVal.getLength());
            count = sourceStream.read(retrievedBuff, 0, nextCopy);

            while (nextCopy > 0 && count != -1) {
                if (calculateMD5) {
                    digest.update(retrievedBuff, 0, count);
                }
                retVal.setLength(retVal.getLength() + count);

                if (retVal.getLength() > abandonLength) {
                    // Abandon operation
                    retVal.setLength(-1);
                    retVal.setMd5(null);
                    break;
                }

                nextCopy = (int) Math.min(retrievedBuff.length, writeLength - retVal.getLength());
                count = sourceStream.read(retrievedBuff, 0, nextCopy);
            }

            if (retVal.getLength() != -1 && calculateMD5) {
                retVal.setMd5(Base64.encode(digest.digest()));
            }

            if (retVal.getLength() != -1 && writeLength > 0) {
                retVal.setLength(Math.min(retVal.getLength(), writeLength));
            }

            if (rewindSourceStream) {
                sourceStream.reset();
                sourceStream.mark(Constants.MAX_MARK_LENGTH);
            }

            return retVal;
        }


        public static boolean areCredentialsEqual(final StorageCredentials thisCred, final StorageCredentials thatCred) {
            if (thisCred == thatCred) {
                return true;
            }

            if (thatCred == null || thisCred.getClass() != thatCred.getClass()) {
                return false;
            }

            if (thisCred instanceof StorageCredentialsAccountAndKey) {
                return ((StorageCredentialsAccountAndKey) thisCred).toString(true).equals(
                        ((StorageCredentialsAccountAndKey) thatCred).toString(true));
            }
            else if (thisCred instanceof StorageCredentialsSharedAccessSignature) {
                return ((StorageCredentialsSharedAccessSignature) thisCred).getToken().equals(
                        ((StorageCredentialsSharedAccessSignature) thatCred).getToken());
            }
            else if (thisCred instanceof StorageCredentialsAnonymous) {
                return true;
            }

            return thisCred.equals(thatCred);
        }


        public static void assertContinuationType(final ResultContinuation continuationToken,
                final ResultContinuationType continuationType) {
            if (continuationToken != null) {
                if (!(continuationToken.getContinuationType() == ResultContinuationType.NONE || continuationToken
                        .getContinuationType() == continuationType)) {
                    final String errorMessage = String.format(Utility.LOCALE_US, SR.UNEXPECTED_CONTINUATION_TYPE,
                            continuationToken.getContinuationType(), continuationType);
                    throw new IllegalArgumentException(errorMessage);
                }
            }
        }


        public static void assertNotNull(final String param, final Object value) {
            if (value == null) {
                throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.ARGUMENT_NULL_OR_EMPTY, param));
            }
        }


        public static void assertNotNullOrEmpty(final String param, final String value) {
            assertNotNull(param, value);

            if (Utility.isNullOrEmpty(value)) {
                throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.ARGUMENT_NULL_OR_EMPTY, param));
            }
        }


        public static void assertInBounds(final String param, final long value, final long min, final long max) {
            if (value < min || value > max) {
                throw new IllegalArgumentException(String.format(SR.PARAMETER_NOT_IN_RANGE, param, min, max));
            }
        }


        public static void assertGreaterThanOrEqual(final String param, final long value, final long min) {
            if (value < min) {
                throw new IllegalArgumentException(String.format(SR.PARAMETER_SHOULD_BE_GREATER_OR_EQUAL, param, min));
            }
        }


        public static boolean validateMaxExecutionTimeout(Long operationExpiryTimeInMs) {
            return validateMaxExecutionTimeout(operationExpiryTimeInMs, 0);
        }


        public static boolean validateMaxExecutionTimeout(Long operationExpiryTimeInMs, long additionalInterval) {
            if (operationExpiryTimeInMs != null) {
                long currentTime = new Date().getTime();
                return operationExpiryTimeInMs < currentTime + additionalInterval;
            }
            return false;
        }


        public static int getRemainingTimeout(Long operationExpiryTimeInMs, Integer timeoutIntervalInMs) throws StorageException {
            if (operationExpiryTimeInMs != null) {
                long remainingTime = operationExpiryTimeInMs - new Date().getTime();
                if (remainingTime > Integer.MAX_VALUE) {
                    return Integer.MAX_VALUE;
                }
                else if (remainingTime > 0) {
                    return (int) remainingTime;
                }
                else {
                    TimeoutException timeoutException = new TimeoutException(SR.MAXIMUM_EXECUTION_TIMEOUT_EXCEPTION);
                    StorageException translatedException = new StorageException(
                            StorageErrorCodeStrings.OPERATION_TIMED_OUT, SR.MAXIMUM_EXECUTION_TIMEOUT_EXCEPTION,
                            Constants.HeaderConstants.HTTP_UNUSED_306, null, timeoutException);
                    throw translatedException;
                }
            }
            else if (timeoutIntervalInMs != null) {
                return timeoutIntervalInMs + Constants.DEFAULT_READ_TIMEOUT;
            }
            else {
                return Constants.DEFAULT_READ_TIMEOUT;
            }
        }


        public static boolean determinePathStyleFromUri(final URI baseURI) {
            String path = baseURI.getPath();
            if (path != null && path.startsWith("/")) {
                path = path.substring(1);
            }

            // if the path is null or empty, this is not path-style
            if (Utility.isNullOrEmpty(path)) {
                return false;
            }

            // if this contains a port or has a host which is not DNS, this is path-style
            return pathStylePorts.contains(baseURI.getPort()) || !isHostDnsName(baseURI);
        }


        private static boolean isHostDnsName(URI uri) {
            String host = uri.getHost();
            for (int i = 0; i < host.length(); i++) {
                char hostChar = host.charAt(i);
                if (!Character.isDigit(hostChar) && !(hostChar == '.')) {
                    return true;
                }
            }
            return false;
        }


        public static String formatETag(final String etag) {
            if (etag.startsWith("\"") && etag.endsWith("\"")) {
                return etag;
            }
            else {
                return String.format("\"%s\"", etag);
            }
        }


        public static StorageException generateNewUnexpectedStorageException(final Exception cause) {
            final StorageException exceptionRef = new StorageException(StorageErrorCode.NONE.toString(),
                    "Unexpected internal storage client error.", 306, // unused
                    null, null);
            exceptionRef.initCause(cause);
            return exceptionRef;
        }


        public static byte[] getBytesFromLong(final long value) {
            final byte[] tempArray = new byte[8];

            for (int m = 0; m < 8; m++) {
                tempArray[7 - m] = (byte) ((value >> (8 * m)) & 0xFF);
            }

            return tempArray;
        }


        public static String getGMTTime() {
            return getGMTTime(new Date());
        }


        public static String getGMTTime(final Date date) {
            final DateFormat formatter = new SimpleDateFormat(RFC1123_PATTERN, LOCALE_US);
            formatter.setTimeZone(GMT_ZONE);
            return formatter.format(date);
        }



        public static String getJavaISO8601Time(Date date) {
            final DateFormat formatter = new SimpleDateFormat(JAVA_ISO8601_PATTERN, LOCALE_US);
            formatter.setTimeZone(UTC_ZONE);
            return formatter.format(date);
        }


        public static SAXParser getSAXParser() throws ParserConfigurationException, SAXException {
            saxParserFactory.setNamespaceAware(true);
            return saxParserFactory.newSAXParser();
        }


        public static String getStandardHeaderValue(final HttpURLConnection conn, final String headerName) {
            final String headerValue = conn.getRequestProperty(headerName);

            // Coalesce null value
            return headerValue == null ? Constants.EMPTY_STRING : headerValue;
        }


        public static String getUTCTimeOrEmpty(final Date value) {
            if (value == null) {
                return Constants.EMPTY_STRING;
            }

            final DateFormat iso8601Format = new SimpleDateFormat(ISO8601_PATTERN, LOCALE_US);
            iso8601Format.setTimeZone(UTC_ZONE);

            return iso8601Format.format(value);
        }


        public static XMLStreamWriter createXMLStreamWriter(StringWriter outWriter) throws XMLStreamException {
            return xmlOutputFactory.createXMLStreamWriter(outWriter);
        }


        public static XMLStreamWriter createXMLStreamWriter(OutputStream outStream, String charset)
                throws XMLStreamException {
            return xmlOutputFactory.createXMLStreamWriter(outStream, charset);
        }


        public static IOException initIOException(final Exception ex) {
            final IOException retEx = new IOException();
            retEx.initCause(ex);
            return retEx;
        }


        public static boolean isNullOrEmpty(final String value) {
            return value == null || value.length() == 0;
        }


        public static boolean isNullOrEmptyOrWhitespace(final String value) {
            return value == null || value.trim().length() == 0;
        }


        public static HashMap<String, String> parseAccountString(final String parseString) {

            // 1. split name value pairs by splitting on the ';' character
            final String[] valuePairs = parseString.split(";");
            final HashMap<String, String> retVals = new HashMap<String, String>();

            // 2. for each field value pair parse into appropriate map entries
            for (int m = 0; m < valuePairs.length; m++) {
                if (valuePairs[m].length() == 0) {
                    continue;
                }
                final int equalDex = valuePairs[m].indexOf("=");
                if (equalDex < 1) {
                    throw new IllegalArgumentException(SR.INVALID_CONNECTION_STRING);
                }

                final String key = valuePairs[m].substring(0, equalDex);
                final String value = valuePairs[m].substring(equalDex + 1);

                // 2.1 add to map
                retVals.put(key, value);
            }

            return retVals;
        }


        public static Date parseRFC1123DateFromStringInGMT(final String value) throws ParseException {
            final DateFormat format = new SimpleDateFormat(RFC1123_PATTERN, Utility.LOCALE_US);
            format.setTimeZone(GMT_ZONE);
            return format.parse(value);
        }


        public static String safeDecode(final String stringToDecode) throws StorageException {
            if (stringToDecode == null) {
                return null;
            }

            if (stringToDecode.length() == 0) {
                return Constants.EMPTY_STRING;
            }

            try {
                if (stringToDecode.contains("+")) {
                    final StringBuilder outBuilder = new StringBuilder();

                    int startDex = 0;
                    for (int m = 0; m < stringToDecode.length(); m++) {
                        if (stringToDecode.charAt(m) == '+') {
                            if (m > startDex) {
                                outBuilder.append(URLDecoder.decode(stringToDecode.substring(startDex, m),
                                        Constants.UTF8_CHARSET));
                            }

                            outBuilder.append("+");
                            startDex = m + 1;
                        }
                    }

                    if (startDex != stringToDecode.length()) {
                        outBuilder.append(URLDecoder.decode(stringToDecode.substring(startDex, stringToDecode.length()),
                                Constants.UTF8_CHARSET));
                    }

                    return outBuilder.toString();
                }
                else {
                    return URLDecoder.decode(stringToDecode, Constants.UTF8_CHARSET);
                }
            }
            catch (final UnsupportedEncodingException e) {
                throw Utility.generateNewUnexpectedStorageException(e);
            }
        }


        public static String safeEncode(final String stringToEncode) throws StorageException {
            if (stringToEncode == null) {
                return null;
            }
            if (stringToEncode.length() == 0) {
                return Constants.EMPTY_STRING;
            }

            try {
                final String tString = URLEncoder.encode(stringToEncode, Constants.UTF8_CHARSET);

                if (stringToEncode.contains(" ")) {
                    final StringBuilder outBuilder = new StringBuilder();

                    int startDex = 0;
                    for (int m = 0; m < stringToEncode.length(); m++) {
                        if (stringToEncode.charAt(m) == ' ') {
                            if (m > startDex) {
                                outBuilder.append(URLEncoder.encode(stringToEncode.substring(startDex, m),
                                        Constants.UTF8_CHARSET));
                            }

                            outBuilder.append("%20");
                            startDex = m + 1;
                        }
                    }

                    if (startDex != stringToEncode.length()) {
                        outBuilder.append(URLEncoder.encode(stringToEncode.substring(startDex, stringToEncode.length()),
                                Constants.UTF8_CHARSET));
                    }

                    return outBuilder.toString();
                }
                else {
                    return tString;
                }

            }
            catch (final UnsupportedEncodingException e) {
                throw Utility.generateNewUnexpectedStorageException(e);
            }
        }


        public static String safeRelativize(final URI baseURI, final URI toUri) throws URISyntaxException {
            // For compatibility followed
            // http://msdn.microsoft.com/en-us/library/system.uri.makerelativeuri.aspx

            // if host and scheme are not identical return from uri
            if (!baseURI.getHost().equals(toUri.getHost()) || !baseURI.getScheme().equals(toUri.getScheme())) {
                return toUri.toString();
            }

            final String basePath = baseURI.getPath();
            String toPath = toUri.getPath();

            int truncatePtr = 1;

            // Seek to first Difference
            // int maxLength = Math.min(basePath.length(), toPath.length());
            int m = 0;
            int ellipsesCount = 0;
            for (; m < basePath.length(); m++) {
                if (m >= toPath.length()) {
                    if (basePath.charAt(m) == '/') {
                        ellipsesCount++;
                    }
                }
                else {
                    if (basePath.charAt(m) != toPath.charAt(m)) {
                        break;
                    }
                    else if (basePath.charAt(m) == '/') {
                        truncatePtr = m + 1;
                    }
                }
            }

            // ../containername and ../containername/{path} should increment the truncatePtr
            // otherwise toPath will incorrectly begin with /containername
            if (m < toPath.length() && toPath.charAt(m) == '/') {
                // this is to handle the empty directory case with the '/' delimiter
                // for example, ../containername/ and ../containername// should not increment the truncatePtr
                if (!(toPath.charAt(m - 1) == '/' && basePath.charAt(m - 1) == '/')) {
                    truncatePtr = m + 1;
                }
            }

            if (m == toPath.length()) {
                // No path difference, return query + fragment
                return new URI(null, null, null, toUri.getQuery(), toUri.getFragment()).toString();
            }
            else {
                toPath = toPath.substring(truncatePtr);
                final StringBuilder sb = new StringBuilder();
                while (ellipsesCount > 0) {
                    sb.append("../");
                    ellipsesCount--;
                }

                if (!Utility.isNullOrEmpty(toPath)) {
                    sb.append(toPath);
                }

                if (!Utility.isNullOrEmpty(toUri.getQuery())) {
                    sb.append("?");
                    sb.append(toUri.getQuery());
                }
                if (!Utility.isNullOrEmpty(toUri.getFragment())) {
                    sb.append("#");
                    sb.append(toUri.getRawFragment());
                }

                return sb.toString();
            }
        }


        public static void logHttpError(StorageException ex, OperationContext opContext) {
            if (Logger.shouldLog(opContext)) {
                try {
                    StringBuilder bld = new StringBuilder();
                    bld.append("Error response received. ");

                    bld.append("HttpStatusCode= ");
                    bld.append(ex.getHttpStatusCode());

                    bld.append(", HttpStatusMessage= ");
                    bld.append(ex.getMessage());

                    bld.append(", ErrorCode= ");
                    bld.append(ex.getErrorCode());

                    StorageExtendedErrorInformation extendedError = ex.getExtendedErrorInformation();
                    if (extendedError != null) {
                        bld.append(", ExtendedErrorInformation= {ErrorMessage= ");
                        bld.append(extendedError.getErrorMessage());

                        HashMap<String, String[]> details = extendedError.getAdditionalDetails();
                        if (details != null) {
                            bld.append(", AdditionalDetails= { ");
                            for (Map.Entry<String, String[]> detail : details.entrySet()) {
                                bld.append(detail.getKey());
                                bld.append("= ");

                                for (String value : detail.getValue()) {
                                    bld.append(value);
                                }
                                bld.append(",");
                            }
                            bld.setCharAt(bld.length() - 1, '}');
                        }
                        bld.append("}");
                    }

                    Logger.debug(opContext, bld.toString());
                } catch (Exception e) {
                    // Do nothing
                }
            }
        }


        public static void logHttpRequest(HttpURLConnection conn, OperationContext opContext) throws IOException {
            if (Logger.shouldLog(opContext)) {
                try {
                    StringBuilder bld = new StringBuilder();

                    bld.append(conn.getRequestMethod());
                    bld.append(" ");
                    bld.append(conn.getURL());
                    bld.append("\n");

                    // The Authorization header will not appear due to a security feature in HttpURLConnection
                    for (Map.Entry<String, List<String>> header : conn.getRequestProperties().entrySet()) {
                        if (header.getKey() != null) {
                            bld.append(header.getKey());
                            bld.append(": ");
                        }

                        for (int i = 0; i < header.getValue().size(); i++) {
                            bld.append(header.getValue().get(i));
                            if (i < header.getValue().size() - 1) {
                                bld.append(",");
                            }
                        }
                        bld.append('\n');
                    }

                    Logger.trace(opContext, bld.toString());
                } catch (Exception e) {
                    // Do nothing
                }
            }
        }


        public static void logHttpResponse(HttpURLConnection conn, OperationContext opContext) throws IOException {
            if (Logger.shouldLog(opContext)) {
                try {
                    StringBuilder bld = new StringBuilder();

                    // This map's null key will contain the response code and message
                    for (Map.Entry<String, List<String>> header : conn.getHeaderFields().entrySet()) {
                        if (header.getKey() != null) {
                            bld.append(header.getKey());
                            bld.append(": ");
                        }

                        for (int i = 0; i < header.getValue().size(); i++) {
                            bld.append(header.getValue().get(i));
                            if (i < header.getValue().size() - 1) {
                                bld.append(",");
                            }
                        }
                        bld.append('\n');
                    }

                    Logger.trace(opContext, bld.toString());
                } catch (Exception e) {
                    // Do nothing
                }
            }
        }


        protected static String trimEnd(final String value, final char trimChar) {
            int stopDex = value.length() - 1;
            while (stopDex > 0 && value.charAt(stopDex) == trimChar) {
                stopDex--;
            }

            return stopDex == value.length() - 1 ? value : value.substring(stopDex);
        }


        public static String trimStart(final String value) {
            int spaceDex = 0;
            while (spaceDex < value.length() && value.charAt(spaceDex) == ' ') {
                spaceDex++;
            }

            return value.substring(spaceDex);
        }


        public static StreamMd5AndLength writeToOutputStream(final InputStream sourceStream, final OutputStream outStream,
                long writeLength, final boolean rewindSourceStream, final boolean calculateMD5, OperationContext opContext,
                final RequestOptions options) throws IOException, StorageException {
            return writeToOutputStream(sourceStream, outStream, writeLength, rewindSourceStream, calculateMD5, opContext,
                    options, null );
        }


        public static StreamMd5AndLength writeToOutputStream(final InputStream sourceStream, final OutputStream outStream,
                long writeLength, final boolean rewindSourceStream, final boolean calculateMD5, OperationContext opContext,
                final RequestOptions options, StorageRequest<?, ?, Integer> request) throws IOException, StorageException {
            if (rewindSourceStream && sourceStream.markSupported()) {
                sourceStream.reset();
                sourceStream.mark(Constants.MAX_MARK_LENGTH);
            }

            final StreamMd5AndLength retVal = new StreamMd5AndLength();

            if (calculateMD5) {
                try {
                    retVal.setDigest(MessageDigest.getInstance("MD5"));
                }
                catch (final NoSuchAlgorithmException e) {
                    // This wont happen, throw fatal.
                    throw Utility.generateNewUnexpectedStorageException(e);
                }
            }

            if (writeLength < 0) {
                writeLength = Long.MAX_VALUE;
            }

            final byte[] retrievedBuff = new byte[Constants.BUFFER_COPY_LENGTH];
            int nextCopy = (int) Math.min(retrievedBuff.length, writeLength);
            int count = sourceStream.read(retrievedBuff, 0, nextCopy);

            while (nextCopy > 0 && count != -1) {

                // if maximum execution time would be exceeded
                if (Utility.validateMaxExecutionTimeout(options.getOperationExpiryTimeInMs())) {
                    // throw an exception
                    TimeoutException timeoutException = new TimeoutException(SR.MAXIMUM_EXECUTION_TIMEOUT_EXCEPTION);
                    throw Utility.initIOException(timeoutException);
                }

                if (outStream != null) {
                    outStream.write(retrievedBuff, 0, count);
                }

                if (calculateMD5) {
                    retVal.getDigest().update(retrievedBuff, 0, count);
                }

                retVal.setLength(retVal.getLength() + count);
                retVal.setCurrentOperationByteCount(retVal.getCurrentOperationByteCount() + count);

                if (request != null) {
                    request.setCurrentRequestByteCount(request.getCurrentRequestByteCount() + count);
                }

                nextCopy = (int) Math.min(retrievedBuff.length, writeLength - retVal.getLength());
                count = sourceStream.read(retrievedBuff, 0, nextCopy);
            }

            if (outStream != null) {
                outStream.flush();
            }

            return retVal;
        }


        private Utility() {
            // No op
        }

        public static void checkNullaryCtor(Class<?> clazzType) {
            Constructor<?> ctor = null;
            try {
                ctor = clazzType.getDeclaredConstructor((Class<?>[]) null);
            }
            catch (Exception e) {
                throw new IllegalArgumentException(SR.MISSING_NULLARY_CONSTRUCTOR);
            }

            if (ctor == null) {
                throw new IllegalArgumentException(SR.MISSING_NULLARY_CONSTRUCTOR);
            }
        }


        public static Date parseDate(String dateString) {
            String pattern = MAX_PRECISION_PATTERN;
            switch(dateString.length()) {
                case 28: // "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'"-> [2012-01-04T23:21:59.1234567Z] length = 28
                case 27: // "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"-> [2012-01-04T23:21:59.123456Z] length = 27
                case 26: // "yyyy-MM-dd'T'HH:mm:ss.SSSSS'Z'"-> [2012-01-04T23:21:59.12345Z] length = 26
                case 25: // "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'"-> [2012-01-04T23:21:59.1234Z] length = 25
                case 24: // "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"-> [2012-01-04T23:21:59.123Z] length = 24
                    dateString = dateString.substring(0, MAX_PRECISION_DATESTRING_LENGTH);
                    break;
                case 23: // "yyyy-MM-dd'T'HH:mm:ss.SS'Z'"-> [2012-01-04T23:21:59.12Z] length = 23
                    // SS is assumed to be milliseconds, so a trailing 0 is necessary
                    dateString = dateString.replace("Z", "0");
                    break;
                case 22: // "yyyy-MM-dd'T'HH:mm:ss.S'Z'"-> [2012-01-04T23:21:59.1Z] length = 22
                    // S is assumed to be milliseconds, so trailing 0's are necessary
                    dateString = dateString.replace("Z", "00");
                    break;
                case 20: // "yyyy-MM-dd'T'HH:mm:ss'Z'"-> [2012-01-04T23:21:59Z] length = 20
                    pattern = Utility.ISO8601_PATTERN;
                    break;
                case 17: // "yyyy-MM-dd'T'HH:mm'Z'"-> [2012-01-04T23:21Z] length = 17
                    pattern = Utility.ISO8601_PATTERN_NO_SECONDS;
                    break;
                default:
                    throw new IllegalArgumentException(String.format(SR.INVALID_DATE_STRING, dateString));
            }

            final DateFormat format = new SimpleDateFormat(pattern, Utility.LOCALE_US);
            format.setTimeZone(UTC_ZONE);
            try {
                return format.parse(dateString);
            }
            catch (final ParseException e) {
                throw new IllegalArgumentException(String.format(SR.INVALID_DATE_STRING, dateString), e);
            }
        }


        public static Date parseDate(String dateString, boolean dateBackwardCompatibility) {
            if (!dateBackwardCompatibility) {
                return parseDate(dateString);
            }

            final int beginMilliIndex = 20;     // Length of "yyyy-MM-ddTHH:mm:ss."
            final int endTenthMilliIndex = 24;  // Length of "yyyy-MM-ddTHH:mm:ss.SSSS"

            // Check whether the millisecond and tenth of a millisecond digits are all 0.
            if (dateString.length() > endTenthMilliIndex &&
                    "0000".equals(dateString.substring(beginMilliIndex, endTenthMilliIndex))) {
                // Remove the millisecond and tenth of a millisecond digits.
                // Treat the final three digits (ticks) as milliseconds.
                dateString = dateString.substring(0, beginMilliIndex) + dateString.substring(endTenthMilliIndex);
            }

            return parseDate(dateString);
        }


        public static RequestLocationMode getListingLocationMode(ResultContinuation token) {
            if ((token != null) && token.getTargetLocation() != null) {
                switch (token.getTargetLocation()) {
                    case PRIMARY:
                        return RequestLocationMode.PRIMARY_ONLY;

                    case SECONDARY:
                        return RequestLocationMode.SECONDARY_ONLY;

                    default:
                        throw new IllegalArgumentException(String.format(SR.ARGUMENT_OUT_OF_RANGE_ERROR, "token",
                                token.getTargetLocation()));
                }
            }

            return RequestLocationMode.PRIMARY_OR_SECONDARY;
        }
    }

    public static class ListingContext {


        private String marker;


        private Integer maxResults;


        private String prefix;


        public ListingContext(final String prefix, final Integer maxResults) {
            this.setPrefix(prefix);
            this.setMaxResults(maxResults);
            this.setMarker(null);
        }


        public final String getMarker() {
            return this.marker;
        }


        public final Integer getMaxResults() {
            return this.maxResults;
        }


        public final String getPrefix() {
            return this.prefix;
        }


        public final void setMarker(final String marker) {
            this.marker = marker;
        }


        protected final void setMaxResults(final Integer maxResults) {
            if (null != maxResults) {
                Utility.assertGreaterThanOrEqual("maxResults", maxResults, 1);
            }

            this.maxResults = maxResults;
        }


        public final void setPrefix(final String prefix) {
            this.prefix = prefix;
        }
    }

    @SuppressWarnings("deprecation")
    public abstract static class StorageRequest<C, P, R> {

        private StorageException exceptionReference;


        private boolean nonExceptionedRetryableFailure;


        private RequestOptions requestOptions;


        private RequestResult result;


        private HttpURLConnection connection;

        private InputStream sendStream;


        private Long offset = null;


        private Long length = null;


        private String lockedETag = null;


        private AccessCondition etagLockCondition = null;


        private boolean arePropertiesPopulated = false;


        private String contentMD5 = null;


        private StorageUri storageUri;


        private LocationMode locationMode;


        private RequestLocationMode requestLocationMode;


        private StorageLocation currentLocation;


        private long currentRequestByteCount = 0;


        private boolean isSent = false;


        protected StorageRequest() {
            // no op
        }


        public StorageRequest(final RequestOptions options, StorageUri storageUri) {
            this.setRequestOptions(options);
            this.setStorageUri(storageUri);
            this.locationMode = LocationMode.PRIMARY_ONLY;
            this.requestLocationMode = RequestLocationMode.PRIMARY_ONLY;
        }


        public final StorageException getException() {
            return this.exceptionReference;
        }


        public final RequestOptions getRequestOptions() {
            return this.requestOptions;
        }


        public final RequestResult getResult() {
            return this.result;
        }


        public final HttpURLConnection getConnection() {
            return this.connection;
        }


        public final InputStream getSendStream() {
            return this.sendStream;
        }


        public Long getOffset() {
            return this.offset;
        }


        public Long getLength() {
            return this.length;
        }


        public final String getLockedETag() {
            return this.lockedETag;
        }


        public final String getContentMD5() {
            return this.contentMD5;
        }


        public LocationMode getLocationMode() {
            return this.locationMode;
        }


        public RequestLocationMode getRequestLocationMode() {
            return this.requestLocationMode;
        }


        public StorageLocation getCurrentLocation() {
            return this.currentLocation;
        }


        public AccessCondition getETagLockCondition() {
            return this.etagLockCondition;
        }


        public boolean getArePropertiesPopulated() {
            return this.arePropertiesPopulated;
        }


        public StorageUri getStorageUri() {
            return this.storageUri;
        }


        public long getCurrentRequestByteCount() {
            return this.currentRequestByteCount;
        }


        protected boolean isSent() {
            return this.isSent;
        }


        protected final void initialize(OperationContext opContext) {
            RequestResult currResult = new RequestResult();
            this.setResult(currResult);
            opContext.appendRequestResult(currResult);

            this.setException(null);
            this.setNonExceptionedRetryableFailure(false);
            this.setIsSent(false);
        }


        public final boolean isNonExceptionedRetryableFailure() {
            return this.nonExceptionedRetryableFailure;
        }


        protected final StorageException materializeException(final OperationContext opContext) {
            if (this.getException() != null) {
                return this.getException();
            }

            return StorageException.translateException(this, null, opContext);
        }

        public static final void signBlobQueueAndFileRequest(HttpURLConnection request, ServiceClient client,
                long contentLength, OperationContext context) throws InvalidKeyException, StorageException {
            if (client.getAuthenticationScheme() == AuthenticationScheme.SHAREDKEYFULL) {
                StorageCredentialsHelper.signBlobAndQueueRequest(client.getCredentials(), request, contentLength, context);
            }
            else {
                StorageCredentialsHelper.signBlobAndQueueRequestLite(client.getCredentials(), request, contentLength,
                        context);
            }
        }

        public static final void signTableRequest(HttpURLConnection request, ServiceClient client, long contentLength,
                OperationContext context) throws InvalidKeyException, StorageException {
            if (client.getAuthenticationScheme() == AuthenticationScheme.SHAREDKEYFULL) {
                StorageCredentialsHelper.signTableRequest(client.getCredentials(), request, contentLength, context);
            }
            else {
                StorageCredentialsHelper.signTableRequestLite(client.getCredentials(), request, contentLength, context);
            }
        }

        public void applyLocationModeToRequest() {
            if (this.getRequestOptions().getLocationMode() != null) {
                this.setLocationMode(this.getRequestOptions().getLocationMode());
            }
        }

        public void initializeLocation() {
            if (this.getStorageUri() != null) {
                switch (this.getLocationMode()) {
                    case PRIMARY_ONLY:
                    case PRIMARY_THEN_SECONDARY:
                        this.setCurrentLocation(StorageLocation.PRIMARY);
                        break;

                    case SECONDARY_ONLY:
                    case SECONDARY_THEN_PRIMARY:
                        this.setCurrentLocation(StorageLocation.SECONDARY);
                        break;

                    default:
                        throw new IllegalArgumentException(String.format(SR.ARGUMENT_OUT_OF_RANGE_ERROR, "locationMode",
                                this.getLocationMode()));
                }
            }
            else {
                this.setCurrentLocation(StorageLocation.PRIMARY);
            }
        }

        @SuppressWarnings("incomplete-switch")
        public void validateLocation() {
            if (this.getStorageUri() != null) {
                if (!this.getStorageUri().validateLocationMode(this.locationMode)) {
                    throw new UnsupportedOperationException(SR.STORAGE_URI_MISSING_LOCATION);
                }
            }

            // If the command only allows for a specific location, we should target
            // that location no matter what the retry policy says.
            switch (this.getRequestLocationMode()) {
                case PRIMARY_ONLY:
                    if (this.getLocationMode() == LocationMode.SECONDARY_ONLY) {
                        throw new IllegalArgumentException(SR.PRIMARY_ONLY_COMMAND);
                    }

                    this.setCurrentLocation(StorageLocation.PRIMARY);
                    this.setLocationMode(LocationMode.PRIMARY_ONLY);
                    break;

                case SECONDARY_ONLY:
                    if (this.getLocationMode() == LocationMode.PRIMARY_ONLY) {
                        throw new IllegalArgumentException(SR.SECONDARY_ONLY_COMMAND);
                    }

                    this.setCurrentLocation(StorageLocation.SECONDARY);
                    this.setLocationMode(LocationMode.SECONDARY_ONLY);
                    break;
            }

            this.getResult().setTargetLocation(this.currentLocation);
        }


        protected final void setException(final StorageException exceptionReference) {
            this.exceptionReference = exceptionReference;
        }


        public final void setNonExceptionedRetryableFailure(final boolean nonExceptionedRetryableFailure) {
            this.nonExceptionedRetryableFailure = nonExceptionedRetryableFailure;
        }


        protected final void setRequestOptions(final RequestOptions requestOptions) {
            this.requestOptions = requestOptions;
        }


        public final void setResult(final RequestResult result) {
            this.result = result;
        }


        public final void setConnection(final HttpURLConnection connection) {
            this.connection = connection;
        }


        public void setSendStream(InputStream sendStream) {
            this.sendStream = sendStream;
        }


        public void setOffset(Long offset) {
            this.offset = offset;
        }


        public void setLength(Long length) {
            this.length = length;
        }


        public void setLockedETag(String lockedETag) {
            this.lockedETag = lockedETag;
        }


        public void setContentMD5(String contentMD5) {
            this.contentMD5 = contentMD5;
        }


        public void setETagLockCondition(AccessCondition etagLockCondition) {
            this.etagLockCondition = etagLockCondition;
        }


        public void setArePropertiesPopulated(boolean arePropertiesPopulated) {
            this.arePropertiesPopulated = arePropertiesPopulated;
        }


        public void setLocationMode(LocationMode locationMode) {
            this.locationMode = locationMode;
        }


        public void setRequestLocationMode(RequestLocationMode requestLocationMode) {
            this.requestLocationMode = requestLocationMode;
        }


        public void setCurrentLocation(StorageLocation currentLocation) {
            this.currentLocation = currentLocation;
        }


        public void setStorageUri(StorageUri storageUri) {
            this.storageUri = storageUri;
        }


        public void setCurrentRequestByteCount(long currentRequestByteCount) {
            this.currentRequestByteCount = currentRequestByteCount;
        }


        public void setRequestLocationMode() {
            // no-op
        }


        protected void setIsSent(boolean isSent) {
            this.isSent = isSent;
        }


        public abstract HttpURLConnection buildRequest(C client, P parentObject, OperationContext context) throws Exception;


        public void setHeaders(HttpURLConnection connection, P parentObject, OperationContext context) {
            // no-op
        }


        public abstract void signRequest(HttpURLConnection connection, C client, OperationContext context) throws Exception;


        public abstract R preProcessResponse(P parentObject, C client, OperationContext context) throws Exception;


        public R postProcessResponse(HttpURLConnection connection, P parentObject, C client, OperationContext context,
                R storageObject) throws Exception {
            return storageObject;
        }


        public void validateStreamWrite(StreamMd5AndLength descriptor) throws StorageException {
            // no-op
        }


        public void recoveryAction(OperationContext context) throws IOException {
            // no-op
        }


        public StorageExtendedErrorInformation parseErrorDetails() {
            try {
                if (this.getConnection() == null || this.getConnection().getErrorStream() == null) {
                    return null;
                }

                return StorageErrorHandler.getExtendedErrorInformation(this.getConnection().getErrorStream());
            } catch (final Exception e) {
                return null;
            }
        }
    }

    public static class StorageCredentialsHelper {

        //
        // RESERVED, for internal use only. Gets a value indicating whether a
        // request can be signed under the Shared Key authentication scheme using
        // the specified credentials.
        //
        // @return <Code>True</Code> if a request can be signed with these
        // credentials; otherwise, <Code>false</Code>
        //

        public static boolean canCredentialsSignRequest(final StorageCredentials creds) {
            if (creds.getClass().equals(StorageCredentialsAccountAndKey.class)) {
                return true;
            }
            else {
                return false;
            }
        }

        //
        // RESERVED, for internal use only. Gets a value indicating whether a
        // request can be signed under the Shared Key Lite authentication scheme
        // using the specified credentials.
        //
        // @return <code>true</code> if a request can be signed with these
        // credentials; otherwise, <code>false</code>
        //

        @Deprecated
        public static boolean canCredentialsSignRequestLite(final StorageCredentials creds) {
            if (creds.getClass().equals(StorageCredentialsAccountAndKey.class)) {
                return true;
            }
            else {
                return false;
            }
        }


        public static String computeHmac256(final StorageCredentials creds, final String value) throws InvalidKeyException {
            return computeHmac256(creds, value, null);
        }


        public static String computeHmac256(final StorageCredentials creds, final String value,
                final OperationContext opContext) throws InvalidKeyException {
            if (creds.getClass().equals(StorageCredentialsAccountAndKey.class)) {
                return StorageKey.computeMacSha256(((StorageCredentialsAccountAndKey) creds).getCredentials().getKey(),
                        value);
            }
            else {
                return null;
            }
        }


        public static void signBlobAndQueueRequest(final StorageCredentials creds,
                final HttpURLConnection request, final long contentLength) throws InvalidKeyException,
                StorageException {
            signBlobAndQueueRequest(creds, request, contentLength, null);
        }


        public static void signBlobAndQueueRequest(final StorageCredentials creds,
                final HttpURLConnection request, final long contentLength, OperationContext opContext)
                throws InvalidKeyException, StorageException {
            if (creds.getClass().equals(StorageCredentialsAccountAndKey.class)) {
                opContext = opContext == null ? new OperationContext() : opContext;
                BaseRequest.signRequestForBlobAndQueue(request, ((StorageCredentialsAccountAndKey) creds).getCredentials(),
                        contentLength, opContext);
            }
        }


        @Deprecated
        public static void signBlobAndQueueRequestLite(final StorageCredentials creds,
                final HttpURLConnection request, final long contentLength) throws InvalidKeyException,
                StorageException {
            signBlobAndQueueRequestLite(creds, request, contentLength, null);
        }


        @Deprecated
        public static void signBlobAndQueueRequestLite(final StorageCredentials creds,
                final HttpURLConnection request, final long contentLength, OperationContext opContext)
                throws StorageException, InvalidKeyException {
            if (creds.getClass().equals(StorageCredentialsAccountAndKey.class)) {
                opContext = opContext == null ? new OperationContext() : opContext;
                BaseRequest.signRequestForBlobAndQueueSharedKeyLite(request,
                        ((StorageCredentialsAccountAndKey) creds).getCredentials(), contentLength, opContext);
            }
        }


        public static void signTableRequest(final StorageCredentials creds, final HttpURLConnection request,
                final long contentLength) throws InvalidKeyException, StorageException {
            signTableRequest(creds, request, contentLength, null);
        }


        public static void signTableRequest(final StorageCredentials creds, final HttpURLConnection request,
                final long contentLength, OperationContext opContext) throws InvalidKeyException, StorageException {
            if (creds.getClass().equals(StorageCredentialsAccountAndKey.class)) {
                opContext = opContext == null ? new OperationContext() : opContext;
                BaseRequest.signRequestForTableSharedKey(request,
                        ((StorageCredentialsAccountAndKey) creds).getCredentials(), contentLength, opContext);
            }
        }


        @Deprecated
        public static void signTableRequestLite(final StorageCredentials creds, final HttpURLConnection request,
                final long contentLength) throws InvalidKeyException, StorageException {
            signTableRequestLite(creds, request, contentLength, null);
        }


        @Deprecated
        public static void signTableRequestLite(final StorageCredentials creds, final HttpURLConnection request,
                final long contentLength, OperationContext opContext) throws StorageException, InvalidKeyException {
            if (creds.getClass().equals(StorageCredentialsAccountAndKey.class)) {
                opContext = opContext == null ? new OperationContext() : opContext;
                BaseRequest.signRequestForTableSharedKeyLite(request,
                        ((StorageCredentialsAccountAndKey) creds).getCredentials(), contentLength, opContext);
            }
        }
    }

    public static final class ExecutionEngine {

        public static <CLIENT_TYPE, PARENT_TYPE, RESULT_TYPE> RESULT_TYPE executeWithRetry(final CLIENT_TYPE client,
                final PARENT_TYPE parentObject, final StorageRequest<CLIENT_TYPE, PARENT_TYPE, RESULT_TYPE> task,
                final RetryPolicyFactory policyFactory, final OperationContext opContext) throws StorageException {

            RetryPolicy policy = null;

            if (policyFactory == null) {
                policy = new RetryNoRetry();
            }
            else {
                policy = policyFactory.createInstance(opContext);

                // if the returned policy is null, set to not retry
                if (policy == null) {
                    policy = new RetryNoRetry();
                }
            }

            int currentRetryCount = 0;
            StorageException translatedException = null;
            HttpURLConnection request = null;
            final long startTime = new Date().getTime();

            while (true) {
                try {
                    // 1-4: setup the request
                    request = setupStorageRequest(client, parentObject, task, currentRetryCount, opContext);

                    Logger.info(opContext, LogConstants.START_REQUEST, request.getURL(),
                            request.getRequestProperty(Constants.HeaderConstants.DATE));

                    // 5. Potentially upload data
                    if (task.getSendStream() != null) {
                        Logger.info(opContext, LogConstants.UPLOAD);
                        final StreamMd5AndLength descriptor = Utility.writeToOutputStream(task.getSendStream(),
                                request.getOutputStream(), task.getLength(), false,
                                false, opContext, task.getRequestOptions());

                        task.validateStreamWrite(descriptor);
                        Logger.info(opContext, LogConstants.UPLOADDONE);
                    }

                    Utility.logHttpRequest(request, opContext);

                    // 6. Process the request - Get response
                    RequestResult currResult = task.getResult();
                    currResult.setStartDate(new Date());

                    Logger.info(opContext, LogConstants.GET_RESPONSE);

                    currResult.setStatusCode(request.getResponseCode());
                    currResult.setStatusMessage(request.getResponseMessage());

                    currResult.setStopDate(new Date());
                    currResult.setServiceRequestID(BaseResponse.getRequestId(request));
                    currResult.setEtag(BaseResponse.getEtag(request));
                    currResult.setRequestDate(BaseResponse.getDate(request));
                    currResult.setContentMD5(BaseResponse.getContentMD5(request));

                    // 7. Fire ResponseReceived Event
                    ExecutionEngine.fireResponseReceivedEvent(opContext, request, task.getResult());

                    Logger.info(opContext, LogConstants.RESPONSE_RECEIVED, currResult.getStatusCode(),
                            currResult.getServiceRequestID(), currResult.getContentMD5(), currResult.getEtag(),
                            currResult.getRequestDate());

                    Utility.logHttpResponse(request, opContext);

                    // 8. Pre-process response to check if there was an exception. Do Response parsing (headers etc).
                    Logger.info(opContext, LogConstants.PRE_PROCESS);
                    RESULT_TYPE result = task.preProcessResponse(parentObject, client, opContext);
                    Logger.info(opContext, LogConstants.PRE_PROCESS_DONE);

                    if (!task.isNonExceptionedRetryableFailure()) {

                        // 9. Post-process response. Read stream from server.
                        Logger.info(opContext, LogConstants.POST_PROCESS);
                        result = task.postProcessResponse(request, parentObject, client, opContext, result);
                        Logger.info(opContext, LogConstants.POST_PROCESS_DONE);

                        // Success return result and drain the input stream.
                        if ((task.getResult().getStatusCode() >= 200) && (task.getResult().getStatusCode() < 300)) {
                            if (request != null) {
                                InputStream inStream = request.getInputStream();
                                // At this point, we already have a result / exception to return to the user.
                                // This is just an optimization to improve socket reuse.
                                try {
                                    Utility.writeToOutputStream(inStream, null, -1, false, false, null,
                                            task.getRequestOptions());
                                }
                                catch (final IOException ex) {
                                }
                                catch (StorageException e) {
                                }
                                finally {
                                    inStream.close();
                                }
                            }
                        }
                        Logger.info(opContext, LogConstants.COMPLETE);

                        return result;
                    }
                    else {
                        Logger.warn(opContext, LogConstants.UNEXPECTED_RESULT_OR_EXCEPTION);
                        // The task may have already parsed an exception.
                        translatedException = task.materializeException(opContext);
                        task.getResult().setException(translatedException);

                        // throw on non retryable status codes: 501, 505, blob type mismatch
                        if (task.getResult().getStatusCode() == HttpURLConnection.HTTP_NOT_IMPLEMENTED
                                || task.getResult().getStatusCode() == HttpURLConnection.HTTP_VERSION
                                || translatedException.getErrorCode().equals(StorageErrorCodeStrings.INVALID_BLOB_TYPE)) {
                            throw translatedException;
                        }
                    }
                }
                catch (final TimeoutException e) {
                    // Retryable
                    Logger.warn(opContext, LogConstants.RETRYABLE_EXCEPTION, e.getClass().getName(), e.getMessage());
                    translatedException = StorageException.translateException(task, e, opContext);
                    task.getResult().setException(translatedException);
                }
                catch (final SocketTimeoutException e) {
                    // Retryable
                    Logger.warn(opContext, LogConstants.RETRYABLE_EXCEPTION, e.getClass().getName(), e.getMessage());
                    translatedException = new StorageException(StorageErrorCodeStrings.OPERATION_TIMED_OUT,
                            "The operation did not complete in the specified time.", -1, null, e);
                    task.getResult().setException(translatedException);
                }
                catch (final IOException e) {
                    // Non Retryable if the inner exception is actually an TimeoutException, otherwise Retryable
                    if (e.getCause() instanceof TimeoutException) {
                        translatedException = new StorageException(StorageErrorCodeStrings.OPERATION_TIMED_OUT,
                                SR.MAXIMUM_EXECUTION_TIMEOUT_EXCEPTION, Constants.HeaderConstants.HTTP_UNUSED_306, null,
                                (Exception) e.getCause());
                        task.getResult().setException(translatedException);
                        Logger.error(opContext, LogConstants.UNRETRYABLE_EXCEPTION, e.getCause().getClass().getName(), e
                                .getCause().getMessage());
                        throw translatedException;
                    }
                    else {
                        Logger.warn(opContext, LogConstants.RETRYABLE_EXCEPTION, e.getClass().getName(), e.getMessage());
                        translatedException = StorageException.translateException(task, e, opContext);
                        task.getResult().setException(translatedException);
                    }
                }
                catch (final XMLStreamException e) {
                    // Non Retryable except when the inner exception is actually an IOException
                    if (e.getCause() instanceof SocketException) {
                        translatedException = StorageException.translateException(task,
                                (Exception) e.getCause(), opContext);
                    }
                    else {
                        translatedException = StorageException.translateException(task, e, opContext);
                    }

                    task.getResult().setException(translatedException);

                    if (!(e.getCause() instanceof IOException)) {
                        Logger.error(opContext, LogConstants.UNRETRYABLE_EXCEPTION, e.getClass().getName(), e.getMessage());
                        throw translatedException;
                    }
                    Logger.warn(opContext, LogConstants.RETRYABLE_EXCEPTION, e.getClass().getName(), e.getMessage());
                }
                catch (final InvalidKeyException e) {
                    // Non Retryable, just throw
                    translatedException = StorageException.translateException(task, e, opContext);
                    task.getResult().setException(translatedException);
                    Logger.error(opContext, LogConstants.UNRETRYABLE_EXCEPTION, e.getClass().getName(), e.getMessage());
                    throw translatedException;
                }
                catch (final URISyntaxException e) {
                    // Non Retryable, just throw
                    translatedException = StorageException.translateException(task, e, opContext);
                    task.getResult().setException(translatedException);
                    Logger.error(opContext, LogConstants.UNRETRYABLE_EXCEPTION, e.getClass().getName(), e.getMessage());
                    throw translatedException;
                }
    //            catch (final TableServiceException e) {
    //                task.getResult().setStatusCode(e.getHttpStatusCode());
    //                task.getResult().setStatusMessage(e.getMessage());
    //                task.getResult().setException(e);
    //
    //                if (!e.isRetryable()) {
    //                    Logger.error(opContext, LogConstants.UNRETRYABLE_EXCEPTION, e.getClass().getName(), e.getMessage());
    //                    throw e;
    //                }
    //                else {
    //                    Logger.warn(opContext, LogConstants.RETRYABLE_EXCEPTION, e.getClass().getName(), e.getMessage());
    //                    translatedException = e;
    //                }
    //            }
                catch (final StorageException e) {
                    // Non Retryable, just throw
                    // do not translate StorageException
                    task.getResult().setException(e);
                    Logger.error(opContext, LogConstants.UNRETRYABLE_EXCEPTION, e.getClass().getName(), e.getMessage());
                    throw e;
                }
                catch (final Exception e) {
                    // Non Retryable, just throw
                    translatedException = StorageException.translateException(task, e, opContext);
                    task.getResult().setException(translatedException);
                    Logger.error(opContext, LogConstants.UNRETRYABLE_EXCEPTION, e.getClass().getName(), e.getMessage());
                    throw translatedException;
                }
                finally {
                    opContext.setClientTimeInMs(new Date().getTime() - startTime);

                    // 10. Fire RequestCompleted Event
                    if (task.isSent()) {
                        ExecutionEngine.fireRequestCompletedEvent(opContext, request, task.getResult());
                    }
                }

                // Evaluate Retry Policy
                Logger.info(opContext, LogConstants.RETRY_CHECK, currentRetryCount, task.getResult().getStatusCode(),
                        translatedException == null ? null : translatedException.getMessage());

                task.setCurrentLocation(getNextLocation(task.getCurrentLocation(), task.getLocationMode()));
                Logger.info(opContext, LogConstants.NEXT_LOCATION, task.getCurrentLocation(), task.getLocationMode());

                RetryContext retryContext = new RetryContext(currentRetryCount++, task.getResult(),
                        task.getCurrentLocation(), task.getLocationMode());

                RetryInfo retryInfo = policy.evaluate(retryContext, opContext);

                if (retryInfo == null) {
                    // policy does not allow for retry
                    Logger.error(opContext, LogConstants.DO_NOT_RETRY_POLICY, translatedException == null ? null
                            : translatedException.getMessage());
                    throw translatedException;
                }
                else if (Utility.validateMaxExecutionTimeout(task.getRequestOptions().getOperationExpiryTimeInMs(),
                        retryInfo.getRetryInterval())) {
                    // maximum execution time would be exceeded by current time plus retry interval delay
                    TimeoutException timeoutException = new TimeoutException(SR.MAXIMUM_EXECUTION_TIMEOUT_EXCEPTION);
                    translatedException = new StorageException(StorageErrorCodeStrings.OPERATION_TIMED_OUT,
                            SR.MAXIMUM_EXECUTION_TIMEOUT_EXCEPTION, Constants.HeaderConstants.HTTP_UNUSED_306, null,
                            timeoutException);

                    task.initialize(opContext);
                    task.getResult().setException(translatedException);

                    Logger.error(opContext, LogConstants.DO_NOT_RETRY_TIMEOUT, translatedException == null ? null
                            : translatedException.getMessage());

                    throw translatedException;
                }
                else {
                    // attempt to retry
                    task.setCurrentLocation(retryInfo.getTargetLocation());
                    task.setLocationMode(retryInfo.getUpdatedLocationMode());
                    Logger.info(opContext, LogConstants.RETRY_INFO, task.getCurrentLocation(), task.getLocationMode());

                    try {
                        ExecutionEngine.fireRetryingEvent(opContext, task.getConnection(), task.getResult(), retryContext);

                        Logger.info(opContext, LogConstants.RETRY_DELAY, retryInfo.getRetryInterval());
                        Thread.sleep(retryInfo.getRetryInterval());
                    }
                    catch (final InterruptedException e) {
                        // Restore the interrupted status
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        private static <CLIENT_TYPE, PARENT_TYPE, RESULT_TYPE> HttpURLConnection setupStorageRequest(
                final CLIENT_TYPE client, final PARENT_TYPE parentObject,
                final StorageRequest<CLIENT_TYPE, PARENT_TYPE, RESULT_TYPE> task, int currentRetryCount,
                final OperationContext opContext) throws StorageException {
            try {

                // reset result flags
                task.initialize(opContext);

                if (Utility.validateMaxExecutionTimeout(task.getRequestOptions().getOperationExpiryTimeInMs())) {
                    // maximum execution time would be exceeded by current time
                    TimeoutException timeoutException = new TimeoutException(SR.MAXIMUM_EXECUTION_TIMEOUT_EXCEPTION);
                    throw new StorageException(StorageErrorCodeStrings.OPERATION_TIMED_OUT,
                            SR.MAXIMUM_EXECUTION_TIMEOUT_EXCEPTION, Constants.HeaderConstants.HTTP_UNUSED_306, null,
                            timeoutException);
                }

                // Run the recovery action if this is a retry. Else, initialize the location mode for the task.
                // For retries, it will be initialized in retry logic.
                if (currentRetryCount > 0) {
                    task.recoveryAction(opContext);
                    Logger.info(opContext, LogConstants.RETRY);
                }
                else {
                    task.applyLocationModeToRequest();
                    task.initializeLocation();
                    Logger.info(opContext, LogConstants.STARTING);
                }

                task.setRequestLocationMode();

                // If the command only allows for a specific location, we should target
                // that location no matter what the retry policy says.
                task.validateLocation();

                Logger.info(opContext, LogConstants.INIT_LOCATION, task.getCurrentLocation(), task.getLocationMode());

                // 1. Build the request
                HttpURLConnection request = task.buildRequest(client, parentObject, opContext);

                // 2. Add headers
                task.setHeaders(request, parentObject, opContext);

                // Add any other custom headers that users have set on the opContext
                if (opContext.getUserHeaders() != null) {
                    for (final Map.Entry<String, String> entry : opContext.getUserHeaders().entrySet()) {
                        request.setRequestProperty(entry.getKey(), entry.getValue());
                    }
                }

                // 3. Fire sending request event
                ExecutionEngine.fireSendingRequestEvent(opContext, request, task.getResult());
                task.setIsSent(true);

                // 4. Sign the request
                task.signRequest(request, client, opContext);

                // set the connection on the task
                task.setConnection(request);

                return request;
            }
            catch (StorageException e) {
                throw e;
            }
            catch (Exception e) {
                throw new StorageException(null, e.getMessage(), Constants.HeaderConstants.HTTP_UNUSED_306, null, e);
            }
        }

        private static StorageLocation getNextLocation(StorageLocation lastLocation, LocationMode locationMode) {
            switch (locationMode) {
                case PRIMARY_ONLY:
                    return StorageLocation.PRIMARY;

                case SECONDARY_ONLY:
                    return StorageLocation.SECONDARY;

                case PRIMARY_THEN_SECONDARY:
                case SECONDARY_THEN_PRIMARY:
                    return (lastLocation == StorageLocation.PRIMARY) ? StorageLocation.SECONDARY : StorageLocation.PRIMARY;

                default:
                    return StorageLocation.PRIMARY;
            }
        }


        private static void fireSendingRequestEvent(OperationContext opContext, HttpURLConnection request,
                RequestResult result) {
            if (opContext.getSendingRequestEventHandler().hasListeners()
                    || OperationContext.getGlobalSendingRequestEventHandler().hasListeners()) {
                SendingRequestEvent event = new SendingRequestEvent(opContext, request, result);
                opContext.getSendingRequestEventHandler().fireEvent(event);
                OperationContext.getGlobalSendingRequestEventHandler().fireEvent(event);
            }
        }


        private static void fireResponseReceivedEvent(OperationContext opContext, HttpURLConnection request,
                RequestResult result) {
            if (opContext.getResponseReceivedEventHandler().hasListeners()
                    || OperationContext.getGlobalResponseReceivedEventHandler().hasListeners()) {
                ResponseReceivedEvent event = new ResponseReceivedEvent(opContext, request, result);
                opContext.getResponseReceivedEventHandler().fireEvent(event);
                OperationContext.getGlobalResponseReceivedEventHandler().fireEvent(event);
            }
        }


        private static void fireRequestCompletedEvent(OperationContext opContext, HttpURLConnection request,
                RequestResult result) {
            if (opContext.getRequestCompletedEventHandler().hasListeners()
                    || OperationContext.getGlobalRequestCompletedEventHandler().hasListeners()) {
                RequestCompletedEvent event = new RequestCompletedEvent(opContext, request, result);
                opContext.getRequestCompletedEventHandler().fireEvent(event);
                OperationContext.getGlobalRequestCompletedEventHandler().fireEvent(event);
            }
        }


        private static void fireRetryingEvent(OperationContext opContext, HttpURLConnection request, RequestResult result,
                RetryContext retryContext) {
            if (opContext.getRetryingEventHandler().hasListeners()
                    || OperationContext.getGlobalRetryingEventHandler().hasListeners()) {
                RetryingEvent event = new RetryingEvent(opContext, request, result, retryContext);
                opContext.getRetryingEventHandler().fireEvent(event);
                OperationContext.getGlobalRetryingEventHandler().fireEvent(event);
            }
        }
    }

    static final class TableFullCanonicalizer extends Canonicalizer {


        @Override
        protected String canonicalize(final HttpURLConnection conn, final String accountName, final Long contentLength) throws StorageException {

            if (contentLength < -1) {
                throw new InvalidParameterException(SR.INVALID_CONTENT_LENGTH);
            }

            return canonicalizeTableHttpRequest(conn.getURL(), accountName, conn.getRequestMethod(),
                    Utility.getStandardHeaderValue(conn, Constants.HeaderConstants.CONTENT_TYPE), contentLength, null,
                    conn);
        }
    }

    public static class BaseResponse {

        public static String getContentMD5(final HttpURLConnection request) {
            return request.getHeaderField(Constants.HeaderConstants.CONTENT_MD5);
        }


        public static String getDate(final HttpURLConnection request) {
            final String retString = request.getHeaderField("Date");
            return retString == null ? request.getHeaderField(Constants.HeaderConstants.DATE) : retString;
        }


        public static String getEtag(final HttpURLConnection request) {
            return request.getHeaderField(Constants.HeaderConstants.ETAG);
        }


        public static HashMap<String, String> getMetadata(final HttpURLConnection request) {
            return getValuesByHeaderPrefix(request, Constants.HeaderConstants.PREFIX_FOR_STORAGE_METADATA);
        }


        public static String getRequestId(final HttpURLConnection request) {
            return request.getHeaderField(Constants.HeaderConstants.REQUEST_ID_HEADER);
        }


        private static HashMap<String, String> getValuesByHeaderPrefix(final HttpURLConnection request, final String prefix) {
            final HashMap<String, String> retVals = new HashMap<String, String>();
            final Map<String, List<String>> headerMap = request.getHeaderFields();
            final int prefixLength = prefix.length();

            for (final Map.Entry<String, List<String>> entry : headerMap.entrySet()) {
                if (entry.getKey() != null && entry.getKey().startsWith(prefix)) {
                    final List<String> currHeaderValues = entry.getValue();
                    retVals.put(entry.getKey().substring(prefixLength), currHeaderValues.get(0));
                }
            }

            return retVals;
        }


        protected BaseResponse() {
            // No op
        }
    }

    public static final class Base64 {

        private static final String BASE_64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";


        private static final byte DECODE_64[] = { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 0-15
                -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
                52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -2, -1, -1,
                -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
                -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
                41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1
        };


        public static byte[] decode(final String data) {
            int byteArrayLength = 3 * data.length() / 4;

            if (data.endsWith("==")) {
                byteArrayLength -= 2;
            }
            else if (data.endsWith("=")) {
                byteArrayLength -= 1;
            }

            final byte[] retArray = new byte[byteArrayLength];
            int byteDex = 0;
            int charDex = 0;

            for (; charDex < data.length(); charDex += 4) {
                // get 4 chars, convert to 3 bytes
                final int char1 = DECODE_64[(byte) data.charAt(charDex)];
                final int char2 = DECODE_64[(byte) data.charAt(charDex + 1)];
                final int char3 = DECODE_64[(byte) data.charAt(charDex + 2)];
                final int char4 = DECODE_64[(byte) data.charAt(charDex + 3)];

                if (char1 < 0 || char2 < 0 || char3 == -1 || char4 == -1) {
                    // invalid character(-1), or bad padding (-2)
                    throw new IllegalArgumentException(SR.STRING_NOT_VALID);
                }

                int tVal = char1 << 18;
                tVal += char2 << 12;
                tVal += (char3 & 0xff) << 6;
                tVal += char4 & 0xff;

                if (char3 == -2) {
                    // two "==" pad chars, check bits 12-24
                    tVal &= 0x00FFF000;
                    retArray[byteDex++] = (byte) (tVal >> 16 & 0xFF);
                }
                else if (char4 == -2) {
                    // one pad char "=" , check bits 6-24.
                    tVal &= 0x00FFFFC0;
                    retArray[byteDex++] = (byte) (tVal >> 16 & 0xFF);
                    retArray[byteDex++] = (byte) (tVal >> 8 & 0xFF);

                }
                else {
                    // No pads take all 3 bytes, bits 0-24
                    retArray[byteDex++] = (byte) (tVal >> 16 & 0xFF);
                    retArray[byteDex++] = (byte) (tVal >> 8 & 0xFF);
                    retArray[byteDex++] = (byte) (tVal & 0xFF);
                }
            }
            return retArray;
        }


        public static Byte[] decodeAsByteObjectArray(final String data) {
            int byteArrayLength = 3 * data.length() / 4;

            if (data.endsWith("==")) {
                byteArrayLength -= 2;
            }
            else if (data.endsWith("=")) {
                byteArrayLength -= 1;
            }

            final Byte[] retArray = new Byte[byteArrayLength];
            int byteDex = 0;
            int charDex = 0;

            for (; charDex < data.length(); charDex += 4) {
                // get 4 chars, convert to 3 bytes
                final int char1 = DECODE_64[(byte) data.charAt(charDex)];
                final int char2 = DECODE_64[(byte) data.charAt(charDex + 1)];
                final int char3 = DECODE_64[(byte) data.charAt(charDex + 2)];
                final int char4 = DECODE_64[(byte) data.charAt(charDex + 3)];

                if (char1 < 0 || char2 < 0 || char3 == -1 || char4 == -1) {
                    // invalid character(-1), or bad padding (-2)
                    throw new IllegalArgumentException(SR.STRING_NOT_VALID);
                }

                int tVal = char1 << 18;
                tVal += char2 << 12;
                tVal += (char3 & 0xff) << 6;
                tVal += char4 & 0xff;

                if (char3 == -2) {
                    // two "==" pad chars, check bits 12-24
                    tVal &= 0x00FFF000;
                    retArray[byteDex++] = (byte) (tVal >> 16 & 0xFF);
                }
                else if (char4 == -2) {
                    // one pad char "=" , check bits 6-24.
                    tVal &= 0x00FFFFC0;
                    retArray[byteDex++] = (byte) (tVal >> 16 & 0xFF);
                    retArray[byteDex++] = (byte) (tVal >> 8 & 0xFF);

                }
                else {
                    // No pads take all 3 bytes, bits 0-24
                    retArray[byteDex++] = (byte) (tVal >> 16 & 0xFF);
                    retArray[byteDex++] = (byte) (tVal >> 8 & 0xFF);
                    retArray[byteDex++] = (byte) (tVal & 0xFF);
                }
            }
            return retArray;
        }


        public static String encode(final byte[] data) {
            final StringBuilder builder = new StringBuilder();
            final int dataRemainder = data.length % 3;

            int j = 0;
            int n = 0;
            for (; j < data.length; j += 3) {

                if (j < data.length - dataRemainder) {
                    n = ((data[j] & 0xFF) << 16) + ((data[j + 1] & 0xFF) << 8) + (data[j + 2] & 0xFF);
                }
                else {
                    if (dataRemainder == 1) {
                        n = (data[j] & 0xFF) << 16;
                    }
                    else if (dataRemainder == 2) {
                        n = ((data[j] & 0xFF) << 16) + ((data[j + 1] & 0xFF) << 8);
                    }
                }

                // Left here for readability
                // byte char1 = (byte) ((n >>> 18) & 0x3F);
                // byte char2 = (byte) ((n >>> 12) & 0x3F);
                // byte char3 = (byte) ((n >>> 6) & 0x3F);
                // byte char4 = (byte) (n & 0x3F);
                builder.append(BASE_64_CHARS.charAt((byte) ((n >>> 18) & 0x3F)));
                builder.append(BASE_64_CHARS.charAt((byte) ((n >>> 12) & 0x3F)));
                builder.append(BASE_64_CHARS.charAt((byte) ((n >>> 6) & 0x3F)));
                builder.append(BASE_64_CHARS.charAt((byte) (n & 0x3F)));
            }

            final int bLength = builder.length();

            // append '=' to pad
            if (data.length % 3 == 1) {
                builder.replace(bLength - 2, bLength, "==");
            }
            else if (data.length % 3 == 2) {
                builder.replace(bLength - 1, bLength, "=");
            }

            return builder.toString();
        }


        public static String encode(final Byte[] data) {
            final StringBuilder builder = new StringBuilder();
            final int dataRemainder = data.length % 3;

            int j = 0;
            int n = 0;
            for (; j < data.length; j += 3) {

                if (j < data.length - dataRemainder) {
                    n = ((data[j] & 0xFF) << 16) + ((data[j + 1] & 0xFF) << 8) + (data[j + 2] & 0xFF);
                }
                else {
                    if (dataRemainder == 1) {
                        n = (data[j] & 0xFF) << 16;
                    }
                    else if (dataRemainder == 2) {
                        n = ((data[j] & 0xFF) << 16) + ((data[j + 1] & 0xFF) << 8);
                    }
                }

                // Left here for readability
                // byte char1 = (byte) ((n >>> 18) & 0x3F);
                // byte char2 = (byte) ((n >>> 12) & 0x3F);
                // byte char3 = (byte) ((n >>> 6) & 0x3F);
                // byte char4 = (byte) (n & 0x3F);
                builder.append(BASE_64_CHARS.charAt((byte) ((n >>> 18) & 0x3F)));
                builder.append(BASE_64_CHARS.charAt((byte) ((n >>> 12) & 0x3F)));
                builder.append(BASE_64_CHARS.charAt((byte) ((n >>> 6) & 0x3F)));
                builder.append(BASE_64_CHARS.charAt((byte) (n & 0x3F)));
            }

            final int bLength = builder.length();

            // append '=' to pad
            if (data.length % 3 == 1) {
                builder.replace(bLength - 2, bLength, "==");
            }
            else if (data.length % 3 == 2) {
                builder.replace(bLength - 1, bLength, "=");
            }

            return builder.toString();
        }


        public static boolean validateIsBase64String(final String data) {

            if (data == null || data.length() % 4 != 0) {
                return false;
            }

            for (int m = 0; m < data.length(); m++) {
                final byte charByte = (byte) data.charAt(m);

                // pad char detected
                if (DECODE_64[charByte] == -2) {
                    if (m < data.length() - 2) {
                        return false;
                    }
                    else if (m == data.length() - 2 && DECODE_64[(byte) data.charAt(m + 1)] != -2) {
                        return false;
                    }
                }

                if (charByte < 0 || DECODE_64[charByte] == -1) {
                    return false;
                }
            }

            return true;
        }


        private Base64() {
            // No op
        }
    }

    public static final class LazySegmentedIterator<CLIENT_TYPE, PARENT_TYPE, ENTITY_TYPE> implements Iterator<ENTITY_TYPE> {


        private ResultSegment<ENTITY_TYPE> currentSegment;


        private Iterator<ENTITY_TYPE> currentSegmentIterator;


        private final CLIENT_TYPE client;


        private final PARENT_TYPE parentObject;


        private final RetryPolicyFactory policyFactory;


        private final StorageRequest<CLIENT_TYPE, PARENT_TYPE, ResultSegment<ENTITY_TYPE>> segmentGenerator;


        private final OperationContext opContext;


        public LazySegmentedIterator(
                final StorageRequest<CLIENT_TYPE, PARENT_TYPE, ResultSegment<ENTITY_TYPE>> segmentGenerator,
                final CLIENT_TYPE client, final PARENT_TYPE parent, final RetryPolicyFactory policyFactory,
                final OperationContext opContext) {
            this.segmentGenerator = segmentGenerator;
            this.parentObject = parent;
            this.opContext = opContext;
            this.policyFactory = policyFactory;
            this.client = client;
        }


        @Override
        @DoesServiceRequest
        public boolean hasNext() {
            while (this.currentSegment == null
                    || (!this.currentSegmentIterator.hasNext() && this.currentSegment != null && this.currentSegment
                            .getHasMoreResults())) {
                try {
                    this.currentSegment = ExecutionEngine.executeWithRetry(this.client, this.parentObject,
                            this.segmentGenerator, this.policyFactory, this.opContext);
                }
                catch (final StorageException e) {
                    final NoSuchElementException ex = new NoSuchElementException(SR.ENUMERATION_ERROR);
                    ex.initCause(e);
                    throw ex;
                }
                this.currentSegmentIterator = this.currentSegment.getResults().iterator();

                if (!this.currentSegmentIterator.hasNext() && !this.currentSegment.getHasMoreResults()) {
                    return false;
                }
            }

            return this.currentSegmentIterator.hasNext();
        }


        @Override
        public ENTITY_TYPE next() {
            if (this.hasNext()) {
                return this.currentSegmentIterator.next();
            } else {
                throw new NoSuchElementException();
            }
        }


        @Override
        public void remove() {
            // read only, no-op
            throw new UnsupportedOperationException();
        }
    }

    public static class ListResponse<T> {

        public static final String ENUMERATION_RESULTS = "EnumerationResults";


        protected ArrayList<T> results = new ArrayList<T>();


        protected String marker;


        protected Integer maxResults;


        protected String nextMarker;


        protected String prefix;


        public ArrayList<T> getResults() {
            return this.results;
        }


        public String getMarker() {
            return this.marker;
        }


        public Integer getMaxResults() {
            return this.maxResults;
        }


        public String getNextMarker() {
            return this.nextMarker;
        }


        public String getPrefix() {
            return this.prefix;
        }


        public void setResults(ArrayList<T> results) {
            this.results = results;
        }


        public void setMarker(String marker) {
            this.marker = marker;
        }


        public void setMaxResults(Integer maxResults) {
            this.maxResults = maxResults;
        }


        public void setNextMarker(String nextMarker) {
            this.nextMarker = nextMarker;
        }


        public void setPrefix(String prefix) {
            this.prefix = prefix;
        }
    }

    public static final class LazySegmentedIterable<CLIENT_TYPE, PARENT_TYPE, ENTITY_TYPE> implements Iterable<ENTITY_TYPE> {

        private final CLIENT_TYPE client;


        private final PARENT_TYPE parentObject;


        private final RetryPolicyFactory policyFactory;


        private final StorageRequest<CLIENT_TYPE, PARENT_TYPE, ResultSegment<ENTITY_TYPE>> segmentGenerator;


        private final OperationContext opContext;

        public LazySegmentedIterable(
                final StorageRequest<CLIENT_TYPE, PARENT_TYPE, ResultSegment<ENTITY_TYPE>> segmentGenerator,
                final CLIENT_TYPE client, final PARENT_TYPE parent, final RetryPolicyFactory policyFactory,
                final OperationContext opContext) {
            this.segmentGenerator = segmentGenerator;
            this.parentObject = parent;
            this.opContext = opContext;
            this.policyFactory = policyFactory;
            this.client = client;
        }

        @Override
        public Iterator<ENTITY_TYPE> iterator() {
            return new LazySegmentedIterator<CLIENT_TYPE, PARENT_TYPE, ENTITY_TYPE>(this.segmentGenerator, this.client,
                    this.parentObject, this.policyFactory, this.opContext);
        }
    }

    static final class StorageErrorHandler extends DefaultHandler {

        private final Stack<String> elementStack = new Stack<String>();
        private StringBuilder bld = new StringBuilder();

        private final StorageExtendedErrorInformation errorInfo = new StorageExtendedErrorInformation();


        public static StorageExtendedErrorInformation getExtendedErrorInformation(final InputStream stream)
                throws SAXException, IOException, ParserConfigurationException {
            SAXParser saxParser = Utility.getSAXParser();
            StorageErrorHandler handler = new StorageErrorHandler();
            saxParser.parse(stream, handler);

            return handler.errorInfo;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            this.elementStack.push(localName);
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            String currentNode = this.elementStack.pop();

            // if the node popped from the stack and the localName don't match, the xml document is improperly formatted
            if (!localName.equals(currentNode)) {
                throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
            }

            String parentNode = null;
            if (!this.elementStack.isEmpty()) {
                parentNode = this.elementStack.peek();
            }

            String value = this.bld.toString();
            if (value.isEmpty()) {
                value = null;
            }

            if (Constants.ERROR_ROOT_ELEMENT.equals(parentNode)) {
                if (Constants.ERROR_CODE.equals(currentNode)) {
                    this.errorInfo.setErrorCode(value);
                }
                else if (Constants.ERROR_MESSAGE.equals(currentNode)) {
                    this.errorInfo.setErrorMessage(value);
                }
                else {
                    // get additional details
                    this.errorInfo.getAdditionalDetails().put(currentNode, new String[] { value });
                }
            }
            else if (Constants.ERROR_EXCEPTION.equals(parentNode)) {
                // get additional details
                this.errorInfo.getAdditionalDetails().put(currentNode, new String[] { value });
            }

            this.bld = new StringBuilder();
        }

        @Override
        public void characters(char ch[], int start, int length) throws SAXException {
            this.bld.append(ch, start, length);
        }
    }

    static class TableLiteCanonicalizer extends Canonicalizer {


        private static final int ExpectedTableLiteCanonicalizedStringLength = 150;


        @Override
        protected String canonicalize(final HttpURLConnection conn, final String accountName, final Long contentLength) throws StorageException {
            if (contentLength < -1) {
                throw new InvalidParameterException(SR.INVALID_CONTENT_LENGTH);
            }

            final String dateString = Utility.getStandardHeaderValue(conn, Constants.HeaderConstants.DATE);
            if (Utility.isNullOrEmpty(dateString)) {
                throw new IllegalArgumentException(SR.MISSING_MANDATORY_DATE_HEADER);
            }
            final StringBuilder canonicalizedString = new StringBuilder(ExpectedTableLiteCanonicalizedStringLength);
            canonicalizedString.append(dateString);
            appendCanonicalizedElement(canonicalizedString, getCanonicalizedResourceLite(conn.getURL(), accountName));

            return canonicalizedString.toString();
        }
    }

    public static class Logger {

        public static void debug(OperationContext opContext, String format) {
            if (shouldLog(opContext)) {
                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isDebugEnabled()) {
                    logger.debug(formatLogEntry(opContext, format));
                }
            }

        }

        public static void debug(OperationContext opContext, String format, Object... args) {
            if (shouldLog(opContext)) {
                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isDebugEnabled()) {
                    logger.debug(formatLogEntry(opContext, format, args));
                }
            }
        }

        public static void debug(OperationContext opContext, String format, Object arg1) {
            if (shouldLog(opContext)) {
                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isDebugEnabled()) {
                    logger.debug(formatLogEntry(opContext, format, arg1));
                }
            }
        }

        public static void debug(OperationContext opContext, String format, Object arg1, Object arg2) {
            if (shouldLog(opContext)) {

                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isDebugEnabled()) {
                    logger.debug(formatLogEntry(opContext, format, arg1, arg2));
                }
            }
        }

        public static void error(OperationContext opContext, String format) {
            if (shouldLog(opContext)) {

                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isErrorEnabled()) {
                    logger.error(formatLogEntry(opContext, format));
                }
            }
        }

        public static void error(OperationContext opContext, String format, Object... args) {
            if (shouldLog(opContext)) {

                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isErrorEnabled()) {
                    logger.error(formatLogEntry(opContext, format, args));
                }
            }
        }

        public static void error(OperationContext opContext, String format, Object arg1) {
            if (shouldLog(opContext)) {

                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isErrorEnabled()) {
                    logger.error(formatLogEntry(opContext, format, arg1));
                }
            }
        }

        public static void error(OperationContext opContext, String format, Object args1, Object args2) {
            if (shouldLog(opContext)) {

                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isErrorEnabled()) {
                    logger.error(formatLogEntry(opContext, format, args1, args2));
                }
            }
        }

        public static void info(OperationContext opContext, String format) {
            if (shouldLog(opContext)) {

                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isInfoEnabled()) {
                    logger.info(formatLogEntry(opContext, format));
                }
            }
        }

        public static void info(OperationContext opContext, String format, Object... args) {
            if (shouldLog(opContext)) {
                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isInfoEnabled()) {
                    logger.info(formatLogEntry(opContext, format, args));
                }
            }
        }

        public static void info(OperationContext opContext, String format, Object arg1) {
            if (shouldLog(opContext)) {

                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isInfoEnabled()) {
                    logger.info(formatLogEntry(opContext, format, arg1));
                }
            }
        }

        public static void info(OperationContext opContext, String format, Object arg1, Object arg2) {
            if (shouldLog(opContext)) {

                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isInfoEnabled()) {
                    logger.info(formatLogEntry(opContext, format, arg1, arg2));
                }
            }
        }

        public static void trace(OperationContext opContext, String format) {
            if (shouldLog(opContext)) {

                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isTraceEnabled()) {
                    logger.trace(formatLogEntry(opContext, format));
                }
            }
        }

        public static void trace(OperationContext opContext, String format, Object... args) {
            if (shouldLog(opContext)) {

                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isTraceEnabled()) {
                    logger.trace(formatLogEntry(opContext, format, args));
                }
            }
        }

        public static void trace(OperationContext opContext, String format, Object arg1) {
            if (shouldLog(opContext)) {

                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isTraceEnabled()) {
                    logger.trace(formatLogEntry(opContext, format, arg1));
                }
            }
        }

        public static void trace(OperationContext opContext, String format, Object arg1, Object arg2) {
            if (shouldLog(opContext)) {

                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isTraceEnabled()) {
                    logger.trace(formatLogEntry(opContext, format, arg1, arg2));
                }
            }
        }

        public static void warn(OperationContext opContext, String format) {
            if (shouldLog(opContext)) {

                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isWarnEnabled()) {
                    logger.warn(formatLogEntry(opContext, format));
                }
            }
        }

        public static void warn(OperationContext opContext, String format, Object... args) {
            if (shouldLog(opContext)) {

                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isWarnEnabled()) {
                    logger.warn(formatLogEntry(opContext, format, args));
                }
            }
        }

        public static void warn(OperationContext opContext, String format, Object arg1) {
            if (shouldLog(opContext)) {

                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isWarnEnabled()) {
                    logger.warn(formatLogEntry(opContext, format, arg1));
                }
            }
        }

        public static void warn(OperationContext opContext, String format, Object arg1, Object arg2) {
            if (shouldLog(opContext)) {

                Log logger = opContext == null ? LogFactory.getLog(OperationContext.defaultLoggerName)
                        : opContext.getLogger();
                if (logger.isWarnEnabled()) {
                    logger.warn(formatLogEntry(opContext, format, arg1, arg2));
                }
            }
        }

        public static boolean shouldLog(OperationContext opContext) {
            if (opContext != null) {
                return opContext.isLoggingEnabled();
            }
            else {
                return OperationContext.isLoggingEnabledByDefault();
            }
        }

        private static String formatLogEntry(OperationContext opContext, String format) {
            return String.format("{%s}: {%s}", (opContext == null) ? "*" : opContext.getClientRequestID(),
                    format.replace('\n', '.'));
        }

        private static String formatLogEntry(OperationContext opContext, String format, Object... args) {
            return String.format("{%s}: {%s}", (opContext == null) ? "*" : opContext.getClientRequestID(),
                    String.format(format, args).replace('\n', '.'));
        }

        private static String formatLogEntry(OperationContext opContext, String format, Object arg1) {
            return String.format("{%s}: {%s}", (opContext == null) ? "*" : opContext.getClientRequestID(),
                    String.format(format, arg1).replace('\n', '.'));
        }

        private static String formatLogEntry(OperationContext opContext, String format, Object arg1, Object arg2) {
            return String.format("{%s}: {%s}", (opContext == null) ? "*" : opContext.getClientRequestID(),
                    String.format(format, arg1, arg2).replace('\n', '.'));
        }

        private Logger() {
            // no op constructor
            // prevents accidental instantiation
        }
    }

    static final class BlobQueueFullCanonicalizer extends Canonicalizer {


        @Override
        protected String canonicalize(final HttpURLConnection conn, final String accountName, final Long contentLength) throws StorageException {

            if (contentLength < -1) {
                throw new InvalidParameterException(SR.INVALID_CONTENT_LENGTH);
            }

            return canonicalizeHttpRequest(conn.getURL(), accountName, conn.getRequestMethod(),
                    Utility.getStandardHeaderValue(conn, Constants.HeaderConstants.CONTENT_TYPE), contentLength, null,
                    conn);
        }
    }

    public static class SR {
        public static final String ACCOUNT_NAME_NULL_OR_EMPTY = "The account name is null or empty.";
        public static final String ARGUMENT_NULL_OR_EMPTY = "The argument must not be null or an empty string. Argument name: %s.";
        public static final String ARGUMENT_OUT_OF_RANGE_ERROR = "The argument is out of range. Argument name: %s, Value passed: %s.";
        public static final String ATTEMPTED_TO_SERIALIZE_INACCESSIBLE_PROPERTY = "An attempt was made to access an inaccessible member of the entity during serialization.";
        public static final String BLOB = "blob";
        public static final String BLOB_DATA_CORRUPTED = "Blob data corrupted (integrity check failed), Expected value is %s, retrieved %s";
        public static final String BLOB_ENDPOINT_NOT_CONFIGURED = "No blob endpoint configured.";
        public static final String BLOB_HASH_MISMATCH = "Blob hash mismatch (integrity check failed), Expected value is %s, retrieved %s.";
        public static final String BLOB_MD5_NOT_SUPPORTED_FOR_PAGE_BLOBS = "Blob level MD5 is not supported for page blobs.";
        public static final String BLOB_TYPE_NOT_DEFINED = "The blob type is not defined.  Allowed types are BlobType.BLOCK_BLOB and BlobType.Page_BLOB.";
        public static final String CANNOT_CREATE_SAS_FOR_GIVEN_CREDENTIALS = "Cannot create Shared Access Signature as the credentials does not have account name information. Please check that the credentials provided support creating Shared Access Signature.";
        public static final String CANNOT_CREATE_SAS_FOR_SNAPSHOTS = "Cannot create Shared Access Signature via references to blob snapshots. Please perform the given operation on the root blob instead.";
        public static final String CANNOT_CREATE_SAS_WITHOUT_ACCOUNT_KEY = "Cannot create Shared Access Signature unless the Account Key credentials are used by the ServiceClient.";
        public static final String CONTAINER = "container";
        public static final String CONTENT_LENGTH_MISMATCH = "An incorrect number of bytes was read from the connection. The connection may have been closed.";
        public static final String CREATING_NETWORK_STREAM = "Creating a NetworkInputStream and expecting to read %s bytes.";
        public static final String CREDENTIALS_CANNOT_SIGN_REQUEST = "CloudBlobClient, CloudQueueClient and CloudTableClient require credentials that can sign a request.";
        public static final String CUSTOM_RESOLVER_THREW = "The custom property resolver delegate threw an exception. Check the inner exception for more details.";
        public static final String DEFAULT_SERVICE_VERSION_ONLY_SET_FOR_BLOB_SERVICE = "DefaultServiceVersion can only be set for the Blob service.";
        public static final String DELETE_SNAPSHOT_NOT_VALID_ERROR = "The option '%s' must be 'None' to delete a specific snapshot specified by '%s'.";
        public static final String DIRECTORY = "directory";
        public static final String EDMTYPE_WAS_NULL = "EdmType cannot be null.";
        public static final String ENUMERATION_ERROR = "An error occurred while enumerating the result, check the original exception for details.";
        public static final String EMPTY_BATCH_NOT_ALLOWED = "Cannot execute an empty batch operation.";
        public static final String ENDPOINT_INFORMATION_UNAVAILABLE = "Endpoint information not available for Account using Shared Access Credentials.";
        public static final String ENTITY_PROPERTY_CANNOT_BE_NULL_FOR_PRIMITIVES = "EntityProperty cannot be set to null for primitive value types.";
        public static final String ETAG_INVALID_FOR_DELETE = "Delete requires a valid ETag (which may be the '*' wildcard).";
        public static final String ETAG_INVALID_FOR_MERGE = "Merge requires a valid ETag (which may be the '*' wildcard).";
        public static final String ETAG_INVALID_FOR_UPDATE = "Replace requires a valid ETag (which may be the '*' wildcard).";
        public static final String EXCEPTION_THROWN_DURING_DESERIALIZATION = "The entity threw an exception during deserialization.";
        public static final String EXCEPTION_THROWN_DURING_SERIALIZATION = "The entity threw an exception during serialization.";
        public static final String EXPECTED_A_FIELD_NAME = "Expected a field name.";
        public static final String EXPECTED_END_ARRAY = "Expected the end of a JSON Array.";
        public static final String EXPECTED_END_OBJECT = "Expected the end of a JSON Object.";
        public static final String EXPECTED_START_ARRAY = "Expected the start of a JSON Array.";
        public static final String EXPECTED_START_ELEMENT_TO_EQUAL_ERROR = "Expected START_ELEMENT to equal error.";
        public static final String EXPECTED_START_OBJECT = "Expected the start of a JSON Object.";
        public static final String FAILED_TO_PARSE_PROPERTY = "Failed to parse property '%s' with value '%s' as type '%s'";
        public static final String FILE = "file";
        public static final String FILE_ENDPOINT_NOT_CONFIGURED = "No file endpoint configured.";
        public static final String FILE_HASH_MISMATCH = "File hash mismatch (integrity check failed), Expected value is %s, retrieved %s.";
        public static final String FILE_MD5_NOT_POSSIBLE = "MD5 cannot be calculated for an existing file because it would require reading the existing data. Please disable StoreFileContentMD5.";
        public static final String INCORRECT_STREAM_LENGTH = "An incorrect stream length was specified, resulting in an authentication failure. Please specify correct length, or -1.";
        public static final String INPUT_STREAM_SHOULD_BE_MARKABLE = "Input stream must be markable.";
        public static final String INVALID_ACCOUNT_NAME = "Invalid account name.";
        public static final String INVALID_ACL_ACCESS_TYPE = "Invalid acl public access type returned '%s'. Expected blob or container.";
        public static final String INVALID_BLOB_TYPE = "Incorrect Blob type, please use the correct Blob type to access a blob on the server. Expected %s, actual %s.";
        public static final String INVALID_BLOCK_ID = "Invalid blockID, blockID must be a valid Base64 String.";
        public static final String INVALID_CONDITIONAL_HEADERS = "The conditionals specified for this operation did not match server.";
        public static final String INVALID_CONNECTION_STRING = "Invalid connection string.";
        public static final String INVALID_CONNECTION_STRING_DEV_STORE_NOT_TRUE = "Invalid connection string, the UseDevelopmentStorage key must always be paired with 'true'.  Remove the flag entirely otherwise.";
        public static final String INVALID_CONTENT_LENGTH = "ContentLength must be set to -1 or positive Long value.";
        public static final String INVALID_CONTENT_TYPE = "An incorrect Content-Type was returned from the server.";
        public static final String INVALID_CORS_RULE = "A CORS rule must contain at least one allowed origin and allowed method, and MaxAgeInSeconds cannot have a value less than zero.";
        public static final String INVALID_DATE_STRING = "Invalid Date String: %s.";
        public static final String INVALID_EDMTYPE_VALUE = "Invalid value '%s' for EdmType.";
        public static final String INVALID_FILE_LENGTH = "File length must be greater than 0 bytes.";
        public static final String INVALID_GEO_REPLICATION_STATUS = "Null or Invalid geo-replication status in response: %s.";
        public static final String INVALID_KEY = "Storage Key is not a valid base64 encoded string.";
        public static final String INVALID_LISTING_DETAILS = "Invalid blob listing details specified.";
        public static final String INVALID_LOGGING_LEVEL = "Invalid logging operations specified.";
        public static final String INVALID_MAX_WRITE_SIZE = "Max write size is 4MB. Please specify a smaller range.";
        public static final String INVALID_MESSAGE_LENGTH = "The message size cannot be larger than %s bytes.";
        public static final String INVALID_MIME_RESPONSE = "Invalid MIME response received.";
        public static final String INVALID_NUMBER_OF_BYTES_IN_THE_BUFFER = "Page data must be a multiple of 512 bytes. Buffer currently contains %d bytes.";
        public static final String INVALID_OPERATION_FOR_A_SNAPSHOT = "Cannot perform this operation on a blob representing a snapshot.";
        public static final String INVALID_PAGE_BLOB_LENGTH = "Page blob length must be multiple of 512.";
        public static final String INVALID_PAGE_START_OFFSET = "Page start offset must be multiple of 512.";
        public static final String INVALID_RANGE_CONTENT_MD5_HEADER = "Cannot specify x-ms-range-get-content-md5 header on ranges larger than 4 MB. Either use a BlobReadStream via openRead, or disable TransactionalMD5 via the BlobRequestOptions.";
        public static final String INVALID_RESOURCE_NAME = "Invalid %s name. Check MSDN for more information about valid naming.";
        public static final String INVALID_RESOURCE_NAME_LENGTH = "Invalid %s name length. The name must be between %s and %s characters long.";
        public static final String INVALID_RESOURCE_RESERVED_NAME = "Invalid %s name. This name is reserved.";
        public static final String INVALID_RESPONSE_RECEIVED = "The response received is invalid or improperly formatted.";
        public static final String INVALID_STORAGE_PROTOCOL_VERSION = "Storage protocol version prior to 2009-09-19 do not support shared key authentication.";
        public static final String INVALID_STORAGE_SERVICE = "Invalid storage service specified.";
        public static final String INVALID_STREAM_LENGTH = "Invalid stream length; stream must be between 0 and %s MB in length.";
        public static final String ITERATOR_EMPTY = "There are no more elements in this enumeration.";
        public static final String KEY_NULL = "Key invalid. Cannot be null.";
        public static final String LEASE_CONDITION_ON_SOURCE = "A lease condition cannot be specified on the source of a copy.";
        public static final String LOG_STREAM_END_ERROR = "Error parsing log record: unexpected end of stream.";
        public static final String LOG_STREAM_DELIMITER_ERROR = "Error parsing log record: unexpected delimiter encountered.";
        public static final String LOG_STREAM_QUOTE_ERROR = "Error parsing log record: unexpected quote character encountered.";
        public static final String LOG_VERSION_UNSUPPORTED = "A storage log version of %s is unsupported.";
        public static final String MARK_EXPIRED = "Stream mark expired.";
        public static final String MAXIMUM_EXECUTION_TIMEOUT_EXCEPTION = "The client could not finish the operation within specified maximum execution timeout.";
        public static final String METADATA_KEY_INVALID = "The key for one of the metadata key-value pairs is null, empty, or whitespace.";
        public static final String METADATA_VALUE_INVALID = "The value for one of the metadata key-value pairs is null, empty, or whitespace.";
        public static final String MISSING_CREDENTIALS = "No credentials provided.";
        public static final String MISSING_MANDATORY_DATE_HEADER = "Canonicalization did not find a non-empty x-ms-date header in the request. Please use a request with a valid x-ms-date header in RFC 123 format.";
        public static final String MISSING_MANDATORY_PARAMETER_FOR_SAS = "Missing mandatory parameters for valid Shared Access Signature.";
        public static final String MISSING_MD5 = "ContentMD5 header is missing in the response.";
        public static final String MISSING_NULLARY_CONSTRUCTOR = "Class type must contain contain a nullary constructor.";
        public static final String OPS_IN_BATCH_MUST_HAVE_SAME_PARTITION_KEY = "All entities in a given batch must have the same partition key.";
        public static final String PARAMETER_NOT_IN_RANGE = "The value of the parameter '%s' should be between %s and %s.";
        public static final String PARAMETER_SHOULD_BE_GREATER = "The value of the parameter '%s' should be greater than %s.";
        public static final String PARAMETER_SHOULD_BE_GREATER_OR_EQUAL = "The value of the parameter '%s' should be greater than or equal to %s.";
        public static final String PARTITIONKEY_MISSING_FOR_DELETE = "Delete requires a partition key.";
        public static final String PARTITIONKEY_MISSING_FOR_MERGE = "Merge requires a partition key.";
        public static final String PARTITIONKEY_MISSING_FOR_UPDATE = "Replace requires a partition key.";
        public static final String PARTITIONKEY_MISSING_FOR_INSERT = "Insert requires a partition key.";
        public static final String PATH_STYLE_URI_MISSING_ACCOUNT_INFORMATION = "Missing account name information inside path style URI. Path style URIs should be of the form http://<IPAddress:Port>/<accountName>";
        public static final String PERMISSIONS_COULD_NOT_BE_PARSED = "Permissions could not be parsed from '%s'.";
        public static final String PRIMARY_ONLY_COMMAND = "This operation can only be executed against the primary storage location.";
        public static final String PROPERTY_CANNOT_BE_SERIALIZED_AS_GIVEN_EDMTYPE = "Property %s with Edm Type %s cannot be de-serialized.";
        public static final String QUERY_PARAMETER_NULL_OR_EMPTY = "Cannot encode a query parameter with a null or empty key.";
        public static final String QUERY_REQUIRES_VALID_CLASSTYPE_OR_RESOLVER = "Query requires a valid class type or resolver.";
        public static final String QUEUE = "queue";
        public static final String QUEUE_ENDPOINT_NOT_CONFIGURED = "No queue endpoint configured.";
        public static final String RELATIVE_ADDRESS_NOT_PERMITTED = "Address %s is a relative address. Only absolute addresses are permitted.";
        public static final String RESOURCE_NAME_EMPTY = "Invalid %s name. The name may not be null, empty, or whitespace only.";
        public static final String RESPONSE_RECEIVED_IS_INVALID = "The response received is invalid or improperly formatted.";
        public static final String RETRIEVE_MUST_BE_ONLY_OPERATION_IN_BATCH = "A batch transaction with a retrieve operation cannot contain any other operations.";
        public static final String ROWKEY_MISSING_FOR_DELETE = "Delete requires a row key.";
        public static final String ROWKEY_MISSING_FOR_MERGE = "Merge requires a row key.";
        public static final String ROWKEY_MISSING_FOR_UPDATE = "Replace requires a row key.";
        public static final String ROWKEY_MISSING_FOR_INSERT = "Insert requires a row key.";
        public static final String SCHEME_NULL_OR_EMPTY = "The protocol to use is null. Please specify whether to use http or https.";
        public static final String SECONDARY_ONLY_COMMAND = "This operation can only be executed against the secondary storage location.";
        public static final String SHARE = "share";
        public static final String SNAPSHOT_LISTING_ERROR = "Listing snapshots is only supported in flat mode (no delimiter). Consider setting useFlatBlobListing to true.";
        public static final String SNAPSHOT_QUERY_OPTION_ALREADY_DEFINED = "Snapshot query parameter is already defined in the blob URI. Either pass in a snapshotTime parameter or use a full URL with a snapshot query parameter.";
        public static final String STORAGE_QUEUE_CREDENTIALS_NULL = "StorageCredentials cannot be null for the Queue service.";
        public static final String STORAGE_TABLE_CREDENTIALS_NULL = "StorageCredentials cannot be null for the Table service.";
        public static final String STORAGE_CLIENT_OR_SAS_REQUIRED = "Either a SAS token or a service client must be specified.";
        public static final String STORAGE_URI_MISSING_LOCATION = "The URI for the target storage location is not specified. Please consider changing the request's location mode.";
        public static final String STORAGE_URI_MUST_MATCH = "Primary and secondary location URIs in a StorageUri must point to the same resource.";
        public static final String STORAGE_URI_NOT_NULL = "Primary and secondary location URIs in a StorageUri must not both be null.";
        public static final String STOREAS_DIFFERENT_FOR_GETTER_AND_SETTER = "StoreAs Annotation found for both getter and setter for property %s with unequal values.";
        public static final String STOREAS_USED_ON_EMPTY_PROPERTY = "StoreAs Annotation found for property %s with empty value.";
        public static final String STREAM_CLOSED = "Stream is already closed.";
        public static final String STREAM_LENGTH_GREATER_THAN_4MB = "Invalid stream length, length must be less than or equal to 4 MB in size.";
        public static final String STREAM_LENGTH_NEGATIVE = "Invalid stream length, specify -1 for unknown length stream, or a positive number of bytes.";
        public static final String STRING_NOT_VALID = "The String is not a valid Base64-encoded string.";
        public static final String TABLE = "table";
        public static final String TABLE_ENDPOINT_NOT_CONFIGURED = "No table endpoint configured.";
        public static final String TABLE_OBJECT_RELATIVE_URIS_NOT_SUPPORTED = "Table Object relative URIs not supported.";
        public static final String TAKE_COUNT_ZERO_OR_NEGATIVE = "Take count must be positive and greater than 0.";
        public static final String TOO_MANY_PATH_SEGMENTS = "The count of URL path segments (strings between '/' characters) as part of the blob name cannot exceed 254.";
        public static final String TOO_MANY_SHARED_ACCESS_POLICY_IDENTIFIERS = "Too many %d shared access policy identifiers provided. Server does not support setting more than %d on a single container, queue, or table.";
        public static final String TOO_MANY_SHARED_ACCESS_POLICY_IDS = "Too many %d shared access policy identifiers provided. Server does not support setting more than %d on a single container.";
        public static final String TYPE_NOT_SUPPORTED = "Type %s is not supported.";
        public static final String UNEXPECTED_CONTINUATION_TYPE = "The continuation type passed in is unexpected. Please verify that the correct continuation type is passed in. Expected {%s}, found {%s}.";
        public static final String UNEXPECTED_FIELD_NAME = "Unexpected field name. Expected: '%s'. Actual: '%s'.";
        public static final String UNEXPECTED_STATUS_CODE_RECEIVED = "Unexpected http status code received.";
        public static final String UNEXPECTED_STREAM_READ_ERROR = "Unexpected error. Stream returned unexpected number of bytes.";
        public static final String UNKNOWN_TABLE_OPERATION = "Unknown table operation.";
    }

    static final class CanonicalizerFactory {

        private static final BlobQueueFullCanonicalizer BLOB_QUEUE_FULL_V2_INSTANCE = new BlobQueueFullCanonicalizer();


        private static final BlobQueueLiteCanonicalizer BLOB_QUEUE_LITE_INSTANCE = new BlobQueueLiteCanonicalizer();


        private static final TableFullCanonicalizer TABLE_FULL_INSTANCE = new TableFullCanonicalizer();


        private static final TableLiteCanonicalizer TABLE_LITE_INSTANCE = new TableLiteCanonicalizer();


        protected static Canonicalizer getBlobQueueFullCanonicalizer(final HttpURLConnection conn) {
            return BLOB_QUEUE_FULL_V2_INSTANCE;
        }


        protected static Canonicalizer getBlobQueueLiteCanonicalizer(final HttpURLConnection conn) {
            return BLOB_QUEUE_LITE_INSTANCE;
        }


        protected static Canonicalizer getTableFullCanonicalizer(final HttpURLConnection conn) {
            return TABLE_FULL_INSTANCE;

        }


        protected static Canonicalizer getTableLiteCanonicalizer(final HttpURLConnection conn) {
            return TABLE_LITE_INSTANCE;
        }


        private CanonicalizerFactory() {
            // No op
        }
    }

    static final class BlockEntryListSerializer {


        public static byte[] writeBlockListToStream(final Iterable<BlockEntry> blockList, final OperationContext opContext)
                throws XMLStreamException, StorageException {
            final StringWriter outWriter = new StringWriter();
            final XMLStreamWriter xmlw = Utility.createXMLStreamWriter(outWriter);

            // default is UTF8
            xmlw.writeStartDocument();
            xmlw.writeStartElement(BlobConstants.BLOCK_LIST_ELEMENT);

            for (final BlockEntry block : blockList) {
                if (block.getSearchMode() == BlockSearchMode.COMMITTED) {
                    xmlw.writeStartElement(BlobConstants.COMMITTED_ELEMENT);
                }
                else if (block.getSearchMode() == BlockSearchMode.UNCOMMITTED) {
                    xmlw.writeStartElement(BlobConstants.UNCOMMITTED_ELEMENT);
                }
                else if (block.getSearchMode() == BlockSearchMode.LATEST) {
                    xmlw.writeStartElement(BlobConstants.LATEST_ELEMENT);
                }

                xmlw.writeCharacters(block.getId());
                xmlw.writeEndElement();
            }

            // end BlockListElement
            xmlw.writeEndElement();

            // end doc
            xmlw.writeEndDocument();
            try {
                return outWriter.toString().getBytes("UTF8");
            }
            catch (final UnsupportedEncodingException e) {
                throw Utility.generateNewUnexpectedStorageException(e);
            }
        }
    }

    public static final class BlobContainerPermissions extends Permissions<SharedAccessBlobPolicy> {


        private BlobContainerPublicAccessType publicAccess;


        public BlobContainerPermissions() {
            super();
            this.setPublicAccess(BlobContainerPublicAccessType.OFF);
        }


        public BlobContainerPublicAccessType getPublicAccess() {
            return this.publicAccess;
        }


        public void setPublicAccess(final BlobContainerPublicAccessType publicAccess) {
            this.publicAccess = publicAccess;
        }
    }

    public static enum BlobContainerPublicAccessType {

        BLOB,


        CONTAINER,


        OFF
    }

    static final class ContainerListHandler extends DefaultHandler {

        private final Stack<String> elementStack = new Stack<String>();
        private StringBuilder bld = new StringBuilder();

        private final CloudBlobClient serviceClient;

        private final ListResponse<CloudBlobContainer> response = new ListResponse<CloudBlobContainer>();
        private BlobContainerAttributes attributes;
        private String containerName;

        private ContainerListHandler(CloudBlobClient serviceClient) {
            this.serviceClient = serviceClient;
        }


        protected static ListResponse<CloudBlobContainer> getContainerList(final InputStream stream,
                final CloudBlobClient serviceClient) throws ParserConfigurationException, SAXException, IOException {
            SAXParser saxParser = Utility.getSAXParser();
            ContainerListHandler handler = new ContainerListHandler(serviceClient);
            saxParser.parse(stream, handler);

            return handler.response;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            this.elementStack.push(localName);

            if (BlobConstants.CONTAINER_ELEMENT.equals(localName)) {
                this.containerName = Constants.EMPTY_STRING;
                this.attributes = new BlobContainerAttributes();
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            String currentNode = this.elementStack.pop();

            // if the node popped from the stack and the localName don't match, the xml document is improperly formatted
            if (!localName.equals(currentNode)) {
                throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
            }

            String parentNode = null;
            if (!this.elementStack.isEmpty()) {
                parentNode = this.elementStack.peek();
            }

            String value = this.bld.toString();
            if (value.isEmpty()) {
                value = null;
            }

            if (BlobConstants.CONTAINER_ELEMENT.equals(currentNode)) {
                try {
                    CloudBlobContainer retContainer = this.serviceClient.getContainerReference(this.containerName);
                    retContainer.setMetadata(this.attributes.getMetadata());
                    retContainer.setProperties(this.attributes.getProperties());

                    this.response.getResults().add(retContainer);
                }
                catch (URISyntaxException e) {
                    throw new SAXException(e);
                }
                catch (StorageException e) {
                    throw new SAXException(e);
                }

            }
            else if (ListResponse.ENUMERATION_RESULTS.equals(parentNode)) {
                if (Constants.PREFIX_ELEMENT.equals(currentNode)) {
                    this.response.setPrefix(value);
                }
                else if (Constants.MARKER_ELEMENT.equals(currentNode)) {
                    this.response.setMarker(value);
                }
                else if (Constants.NEXT_MARKER_ELEMENT.equals(currentNode)) {
                    this.response.setNextMarker(value);
                }
                else if (Constants.MAX_RESULTS_ELEMENT.equals(currentNode)) {
                    this.response.setMaxResults(Integer.parseInt(value));
                }
            }
            else if (BlobConstants.CONTAINER_ELEMENT.equals(parentNode)) {
                if (Constants.NAME_ELEMENT.equals(currentNode)) {
                    this.containerName = value;
                }
            }
            else if (Constants.PROPERTIES.equals(parentNode)) {
                try {
                    getProperties(currentNode, value);
                }
                catch (ParseException e) {
                    throw new SAXException(e);
                }
            }
            else if (Constants.METADATA_ELEMENT.equals(parentNode)) {
                if (value == null) {
                    value = "";
                }
                this.attributes.getMetadata().put(currentNode, value);
            }

            this.bld = new StringBuilder();
        }

        @Override
        public void characters(char ch[], int start, int length) throws SAXException {
            this.bld.append(ch, start, length);
        }

        private void getProperties(String currentNode, String value) throws ParseException, SAXException {

            if (currentNode.equals(Constants.LAST_MODIFIED_ELEMENT)) {
                this.attributes.getProperties().setLastModified(Utility.parseRFC1123DateFromStringInGMT(value));
            }
            else if (currentNode.equals(Constants.ETAG_ELEMENT)) {
                this.attributes.getProperties().setEtag(Utility.formatETag(value));
            }
            else if (currentNode.equals(Constants.LEASE_STATUS_ELEMENT)) {
                final LeaseStatus tempStatus = LeaseStatus.parse(value);
                if (!tempStatus.equals(LeaseStatus.UNSPECIFIED)) {
                    this.attributes.getProperties().setLeaseStatus(tempStatus);
                }
                else {
                    throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
                }
            }
            else if (currentNode.equals(Constants.LEASE_STATE_ELEMENT)) {
                final LeaseState tempState = LeaseState.parse(value);
                if (!tempState.equals(LeaseState.UNSPECIFIED)) {
                    this.attributes.getProperties().setLeaseState(tempState);
                }
                else {
                    throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
                }
            }
            else if (currentNode.equals(Constants.LEASE_DURATION_ELEMENT)) {
                final LeaseDuration tempDuration = LeaseDuration.parse(value);
                if (!tempDuration.equals(LeaseDuration.UNSPECIFIED)) {
                    this.attributes.getProperties().setLeaseDuration(tempDuration);
                }
                else {
                    throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
                }
            }
        }
    }

    public static final class BlobContainerProperties {


        private String etag;


        private Date lastModified;


        private LeaseStatus leaseStatus;


        private LeaseState leaseState;


        private LeaseDuration leaseDuration;


        public String getEtag() {
            return this.etag;
        }


        public Date getLastModified() {
            return this.lastModified;
        }


        public LeaseStatus getLeaseStatus() {
            return this.leaseStatus;
        }


        public LeaseState getLeaseState() {
            return this.leaseState;
        }


        public LeaseDuration getLeaseDuration() {
            return this.leaseDuration;
        }


        protected void setEtag(final String etag) {
            this.etag = etag;
        }


        protected void setLastModified(final Date lastModified) {
            this.lastModified = lastModified;
        }


        protected void setLeaseStatus(final LeaseStatus leaseStatus) {
            this.leaseStatus = leaseStatus;
        }


        protected void setLeaseState(final LeaseState leaseState) {
            this.leaseState = leaseState;
        }


        protected void setLeaseDuration(final LeaseDuration leaseDuration) {
            this.leaseDuration = leaseDuration;
        }
    }

    static final class ListBlobsResponse extends ListResponse<ListBlobItem> {


        private String delimiter;


        public String getDelimiter() {
            return this.delimiter;
        }


        public void setDelimiter(String delimiter) {
            this.delimiter = delimiter;
        }
    }

    public static final class CloudBlockBlob extends CloudBlob {


        public CloudBlockBlob(final URI blobAbsoluteUri) throws StorageException {
            this(new StorageUri(blobAbsoluteUri));
        }


        public CloudBlockBlob(final StorageUri blobAbsoluteUri) throws StorageException {
            super(BlobType.BLOCK_BLOB);

            Utility.assertNotNull("blobAbsoluteUri", blobAbsoluteUri);
            this.setStorageUri(blobAbsoluteUri);
            this.parseURIQueryStringAndVerify(blobAbsoluteUri, null,
                    Utility.determinePathStyleFromUri(blobAbsoluteUri.getPrimaryUri()));
        }


        public CloudBlockBlob(final CloudBlockBlob otherBlob) {
            super(otherBlob);
        }


        public CloudBlockBlob(final URI blobAbsoluteUri, final CloudBlobClient client) throws StorageException {
            this(new StorageUri(blobAbsoluteUri), client);
        }


        public CloudBlockBlob(final StorageUri blobAbsoluteUri, final CloudBlobClient client) throws StorageException {
            super(BlobType.BLOCK_BLOB, blobAbsoluteUri, client);
        }


        public CloudBlockBlob(final URI blobAbsoluteUri, final CloudBlobClient client, final CloudBlobContainer container)
                throws StorageException {
            this(new StorageUri(blobAbsoluteUri), client, container);
        }


        public CloudBlockBlob(final StorageUri blobAbsoluteUri, final CloudBlobClient client,
                final CloudBlobContainer container) throws StorageException {
            super(BlobType.BLOCK_BLOB, blobAbsoluteUri, client, container);
        }


        public CloudBlockBlob(final URI blobAbsoluteUri, final String snapshotID, final CloudBlobClient client)
                throws StorageException {
            this(new StorageUri(blobAbsoluteUri), snapshotID, client);
        }


        public CloudBlockBlob(final StorageUri blobAbsoluteUri, final String snapshotID, final CloudBlobClient client)
                throws StorageException {
            super(BlobType.BLOCK_BLOB, blobAbsoluteUri, snapshotID, client);
        }


        @DoesServiceRequest
        public void commitBlockList(final Iterable<BlockEntry> blockList) throws StorageException {
            this.commitBlockList(blockList, null , null , null );
        }


        @DoesServiceRequest
        public void commitBlockList(final Iterable<BlockEntry> blockList, final AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException {
            assertNoWriteOperationForSnapshot();

            if (opContext == null) {
                opContext = new OperationContext();
            }

            options = BlobRequestOptions.applyDefaults(options, BlobType.BLOCK_BLOB, this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.commitBlockListImpl(blockList, accessCondition, options, opContext),
                    options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, Void> commitBlockListImpl(final Iterable<BlockEntry> blockList,
                final AccessCondition accessCondition, final BlobRequestOptions options, final OperationContext opContext)
                throws StorageException {

            byte[] blockListBytes;
            try {
                blockListBytes = BlockEntryListSerializer.writeBlockListToStream(blockList, opContext);

                final ByteArrayInputStream blockListInputStream = new ByteArrayInputStream(blockListBytes);

                // This also marks the stream. Therefore no need to mark it in buildRequest.
                final StreamMd5AndLength descriptor = Utility.analyzeStream(blockListInputStream, -1L, -1L,
                        true, options.getUseTransactionalContentMD5());

                final StorageRequest<CloudBlobClient, CloudBlob, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, Void>(
                        options, this.getStorageUri()) {

                    @Override
                    public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                            throws Exception {
                        this.setSendStream(blockListInputStream);
                        this.setLength(descriptor.getLength());
                        return BlobRequest.putBlockList(
                                blob.getTransformedAddress(context).getUri(this.getCurrentLocation()), options, context,
                                accessCondition, blob.properties);
                    }

                    @Override
                    public void setHeaders(HttpURLConnection connection, CloudBlob blob, OperationContext context) {
                        BlobRequest.addMetadata(connection, blob.metadata, context);

                        if (options.getUseTransactionalContentMD5()) {
                            connection.setRequestProperty(Constants.HeaderConstants.CONTENT_MD5, descriptor.getMd5());
                        }
                    }

                    @Override
                    public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                            throws Exception {
                        StorageRequest.signBlobQueueAndFileRequest(connection, client, this.getLength(), null);
                    }

                    @Override
                    public Void preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                            throws Exception {
                        if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_CREATED) {
                            this.setNonExceptionedRetryableFailure(true);
                            return null;
                        }

                        blob.updateEtagAndLastModifiedFromResponse(this.getConnection());
                        return null;
                    }

                    @Override
                    public void recoveryAction(OperationContext context) throws IOException {
                        blockListInputStream.reset();
                        blockListInputStream.mark(Constants.MAX_MARK_LENGTH);
                    }
                };

                return putRequest;
            }
            catch (XMLStreamException e) {
                // The request was not even made. There was an error while trying to write the block list. Just throw.
                StorageException translatedException = StorageException.translateClientException(e);
                throw translatedException;
            }
            catch (IOException e) {
                // The request was not even made. There was an error while trying to write the block list. Just throw.
                StorageException translatedException = StorageException.translateClientException(e);
                throw translatedException;
            }
        }


        @DoesServiceRequest
        public ArrayList<BlockEntry> downloadBlockList() throws StorageException {
            return this.downloadBlockList(BlockListingFilter.COMMITTED, null , null ,
                    null );
        }


        @DoesServiceRequest
        public ArrayList<BlockEntry> downloadBlockList(final BlockListingFilter blockListingFilter,
                final AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
                throws StorageException {
            Utility.assertNotNull("blockListingFilter", blockListingFilter);

            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.BLOCK_BLOB, this.blobServiceClient);

            return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.downloadBlockListImpl(blockListingFilter, accessCondition, options),
                    options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, ArrayList<BlockEntry>> downloadBlockListImpl(
                final BlockListingFilter blockListingFilter, final AccessCondition accessCondition,
                final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlob, ArrayList<BlockEntry>> getRequest = new StorageRequest<CloudBlobClient, CloudBlob, ArrayList<BlockEntry>>(
                    options, this.getStorageUri()) {

                @Override
                public void setRequestLocationMode() {
                    this.setRequestLocationMode(RequestLocationMode.PRIMARY_OR_SECONDARY);
                }

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    return BlobRequest.getBlockList(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                            options, context, accessCondition, blob.snapshotID, blockListingFilter);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, -1L, null);
                }

                @Override
                public ArrayList<BlockEntry> preProcessResponse(CloudBlob blob, CloudBlobClient client,
                        OperationContext context) throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                    }

                    return null;
                }

                @Override
                public ArrayList<BlockEntry> postProcessResponse(HttpURLConnection connection, CloudBlob blob,
                        CloudBlobClient client, OperationContext context, ArrayList<BlockEntry> storageObject)
                        throws Exception {
                    blob.updateEtagAndLastModifiedFromResponse(this.getConnection());
                    blob.updateLengthFromResponse(this.getConnection());

                    return BlockListHandler.getBlockList(this.getConnection().getInputStream());
                }
            };

            return getRequest;
        }


        public BlobOutputStream openOutputStream() throws StorageException {
            return this.openOutputStream(null , null , null );
        }


        public BlobOutputStream openOutputStream(final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            assertNoWriteOperationForSnapshot();

            options = BlobRequestOptions.applyDefaults(options, BlobType.BLOCK_BLOB, this.blobServiceClient,
                    false );

            return new BlobOutputStream(this, accessCondition, options, opContext);
        }


        @Override
        @DoesServiceRequest
        public void upload(final InputStream sourceStream, final long length) throws StorageException, IOException {
            this.upload(sourceStream, length, null , null , null );
        }


        @Override
        @DoesServiceRequest
        public void upload(final InputStream sourceStream, final long length, final AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException, IOException {
            if (length < -1) {
                throw new IllegalArgumentException(SR.STREAM_LENGTH_NEGATIVE);
            }

            assertNoWriteOperationForSnapshot();

            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.BLOCK_BLOB, this.blobServiceClient);

            StreamMd5AndLength descriptor = new StreamMd5AndLength();
            descriptor.setLength(length);

            if (sourceStream.markSupported()) {
                // Mark sourceStream for current position.
                sourceStream.mark(Constants.MAX_MARK_LENGTH);
            }

            // If the stream is rewindable and the length is unknown or we need to
            // set md5, then analyze the stream.
            // Note this read will abort at
            // options.getSingleBlobPutThresholdInBytes() bytes and return
            // -1 as length in which case we will revert to using a stream as it is
            // over the single put threshold.
            if (sourceStream.markSupported()
                    && (length < 0 || (options.getStoreBlobContentMD5() && length <= options
                            .getSingleBlobPutThresholdInBytes()))) {
                // If the stream is of unknown length or we need to calculate
                // the MD5, then we we need to read the stream contents first

                descriptor = Utility.analyzeStream(sourceStream, length, options.getSingleBlobPutThresholdInBytes() + 1,
                        true, options.getStoreBlobContentMD5());

                if (descriptor.getMd5() != null && options.getStoreBlobContentMD5()) {
                    this.properties.setContentMD5(descriptor.getMd5());
                }
            }

            // If the stream is rewindable, and the length is known and less than
            // threshold the upload in a single put, otherwise use a stream.
            if (sourceStream.markSupported() && descriptor.getLength() != -1
                    && descriptor.getLength() < options.getSingleBlobPutThresholdInBytes() + 1) {
                this.uploadFullBlob(sourceStream, descriptor.getLength(), accessCondition, options, opContext);
            }
            else {
                final BlobOutputStream writeStream = this.openOutputStream(accessCondition, options, opContext);
                try {
                    writeStream.write(sourceStream, length);
                }
                finally {
                    writeStream.close();
                }
            }
        }


        @DoesServiceRequest
        protected final void uploadFullBlob(final InputStream sourceStream, final long length,
                final AccessCondition accessCondition, final BlobRequestOptions options, final OperationContext opContext)
                throws StorageException {
            assertNoWriteOperationForSnapshot();

            // Mark sourceStream for current position.
            sourceStream.mark(Constants.MAX_MARK_LENGTH);

            if (length < 0 || length > BlobConstants.MAX_SINGLE_UPLOAD_BLOB_SIZE_IN_BYTES) {
                throw new IllegalArgumentException(String.format(SR.INVALID_STREAM_LENGTH,
                        BlobConstants.MAX_SINGLE_UPLOAD_BLOB_SIZE_IN_BYTES / Constants.MB));
            }

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    uploadFullBlobImpl(sourceStream, length, accessCondition, options, opContext),
                    options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, Void> uploadFullBlobImpl(final InputStream sourceStream,
                final long length, final AccessCondition accessCondition, final BlobRequestOptions options,
                final OperationContext opContext) {
            final StorageRequest<CloudBlobClient, CloudBlob, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, Void>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    this.setSendStream(sourceStream);
                    this.setLength(length);
                    return BlobRequest.putBlob(blob.getTransformedAddress(opContext).getUri(this.getCurrentLocation()),
                            options, opContext, accessCondition, blob.properties, blob.properties.getBlobType(),
                            this.getLength());
                }

                @Override
                public void setHeaders(HttpURLConnection connection, CloudBlob blob, OperationContext context) {
                    BlobRequest.addMetadata(connection, blob.metadata, opContext);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, length, null);
                }

                @Override
                public Void preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_CREATED) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    blob.updateEtagAndLastModifiedFromResponse(this.getConnection());
                    return null;
                }

                @Override
                public void recoveryAction(OperationContext context) throws IOException {
                    sourceStream.reset();
                    sourceStream.mark(Constants.MAX_MARK_LENGTH);
                }

                @Override
                public void validateStreamWrite(StreamMd5AndLength descriptor) throws StorageException {
                    if (this.getLength() != null && this.getLength() != -1) {
                        if (length != descriptor.getLength()) {
                            throw new StorageException(StorageErrorCodeStrings.INVALID_INPUT, SR.INCORRECT_STREAM_LENGTH,
                                    HttpURLConnection.HTTP_FORBIDDEN, null, null);
                        }
                    }
                }
            };

            return putRequest;
        }


        @DoesServiceRequest
        public void uploadBlock(final String blockId, final InputStream sourceStream, final long length)
                throws StorageException, IOException {
            this.uploadBlock(blockId, sourceStream, length, null , null , null );
        }


        @DoesServiceRequest
        public void uploadBlock(final String blockId, final InputStream sourceStream, final long length,
                final AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
                throws StorageException, IOException {
            if (length < -1) {
                throw new IllegalArgumentException(SR.STREAM_LENGTH_NEGATIVE);
            }

            if (length > 4 * Constants.MB) {
                throw new IllegalArgumentException(SR.STREAM_LENGTH_GREATER_THAN_4MB);
            }

            assertNoWriteOperationForSnapshot();

            if (opContext == null) {
                opContext = new OperationContext();
            }

            options = BlobRequestOptions.applyDefaults(options, BlobType.BLOCK_BLOB, this.blobServiceClient);

            // Assert block length
            if (Utility.isNullOrEmpty(blockId) || !Base64.validateIsBase64String(blockId)) {
                throw new IllegalArgumentException(SR.INVALID_BLOCK_ID);
            }

            if (sourceStream.markSupported()) {
                // Mark sourceStream for current position.
                sourceStream.mark(Constants.MAX_MARK_LENGTH);
            }

            InputStream bufferedStreamReference = sourceStream;
            StreamMd5AndLength descriptor = new StreamMd5AndLength();
            descriptor.setLength(length);

            if (!sourceStream.markSupported()) {
                // needs buffering
                final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                descriptor = Utility.writeToOutputStream(sourceStream, byteStream, length, false,
                        options.getUseTransactionalContentMD5(), opContext, options);

                bufferedStreamReference = new ByteArrayInputStream(byteStream.toByteArray());
            }
            else if (length < 0 || options.getUseTransactionalContentMD5()) {
                // If the stream is of unknown length or we need to calculate the
                // MD5, then we we need to read the stream contents first

                descriptor = Utility.analyzeStream(sourceStream, length, -1L, true,
                        options.getUseTransactionalContentMD5());
            }

            if (descriptor.getLength() > 4 * Constants.MB) {
                throw new IllegalArgumentException(SR.STREAM_LENGTH_GREATER_THAN_4MB);
            }

            this.uploadBlockInternal(blockId, descriptor.getMd5(), bufferedStreamReference, descriptor.getLength(),
                    accessCondition, options, opContext);
        }


        @DoesServiceRequest
        private void uploadBlockInternal(final String blockId, final String md5, final InputStream sourceStream,
                final long length, final AccessCondition accessCondition, final BlobRequestOptions options,
                final OperationContext opContext) throws StorageException {
            ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    uploadBlockImpl(blockId, md5, sourceStream, length, accessCondition, options, opContext),
                    options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, Void> uploadBlockImpl(final String blockId, final String md5,
                final InputStream sourceStream, final long length, final AccessCondition accessCondition,
                final BlobRequestOptions options, final OperationContext opContext) {

            final StorageRequest<CloudBlobClient, CloudBlob, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, Void>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    this.setSendStream(sourceStream);
                    this.setLength(length);
                    return BlobRequest.putBlock(blob.getTransformedAddress(opContext).getUri(this.getCurrentLocation()),
                            options, opContext, accessCondition, blockId);
                }

                @Override
                public void setHeaders(HttpURLConnection connection, CloudBlob blob, OperationContext context) {
                    if (options.getUseTransactionalContentMD5()) {
                        connection.setRequestProperty(Constants.HeaderConstants.CONTENT_MD5, md5);
                    }
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, length, null);
                }

                @Override
                public Void preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_CREATED) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    return null;
                }

                @Override
                public void recoveryAction(OperationContext context) throws IOException {
                    sourceStream.reset();
                    sourceStream.mark(Constants.MAX_MARK_LENGTH);
                }
            };

            return putRequest;
        }


        public void uploadText(final String content) throws StorageException, IOException {
            this.uploadText(content, null , null , null , null );
        }


        public void uploadText(final String content, final String charsetName, final AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException, IOException {
            byte[] bytes = (charsetName == null) ? content.getBytes() : content.getBytes(charsetName);
            this.uploadFromByteArray(bytes, 0, bytes.length, accessCondition, options, opContext);
        }


        public String downloadText() throws StorageException, IOException {
            return this
                    .downloadText(null , null , null , null );
        }


        public String downloadText(final String charsetName, final AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException, IOException {

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            this.download(baos, accessCondition, options, opContext);
            return charsetName == null ? baos.toString() : baos.toString(charsetName);
        }


        @Override
        public void setStreamWriteSizeInBytes(final int streamWriteSizeInBytes) {
            if (streamWriteSizeInBytes > Constants.MAX_BLOCK_SIZE || streamWriteSizeInBytes < 16 * Constants.KB) {
                throw new IllegalArgumentException("StreamWriteSizeInBytes");
            }

            this.streamWriteSizeInBytes = streamWriteSizeInBytes;
        }
    }

    public static final class BlobProperties {


        private BlobType blobType = BlobType.UNSPECIFIED;


        private String cacheControl;


        private String contentDisposition;


        private String contentEncoding;


        private String contentLanguage;


        private String contentMD5;


        private String contentType;


        private CopyState copyState;


        private String etag;


        private Date lastModified;


        private LeaseStatus leaseStatus = LeaseStatus.UNLOCKED;


        private LeaseState leaseState;


        private LeaseDuration leaseDuration;


        private long length;


        private Long pageBlobSequenceNumber;


        public BlobProperties() {
            // No op
        }


        public BlobProperties(final BlobProperties other) {
            this.blobType = other.blobType;
            this.contentDisposition = other.contentDisposition;
            this.contentEncoding = other.contentEncoding;
            this.contentLanguage = other.contentLanguage;
            this.contentType = other.contentType;
            this.copyState = other.copyState;
            this.etag = other.etag;
            this.leaseStatus = other.leaseStatus;
            this.leaseState = other.leaseState;
            this.leaseDuration = other.leaseDuration;
            this.length = other.length;
            this.lastModified = other.lastModified;
            this.contentMD5 = other.contentMD5;
            this.cacheControl = other.cacheControl;
            this.pageBlobSequenceNumber = other.pageBlobSequenceNumber;
        }


        public BlobProperties(final BlobType type) {
            this.blobType = type;
        }


        public BlobType getBlobType() {
            return this.blobType;
        }


        public String getCacheControl() {
            return this.cacheControl;
        }


        public String getContentDisposition() {
            return this.contentDisposition;
        }


        public String getContentEncoding() {
            return this.contentEncoding;
        }


        public String getContentLanguage() {
            return this.contentLanguage;
        }


        public String getContentMD5() {
            return this.contentMD5;
        }


        public String getContentType() {
            return this.contentType;
        }


        public CopyState getCopyState() {
            return this.copyState;
        }


        public String getEtag() {
            return this.etag;
        }


        public Date getLastModified() {
            return this.lastModified;
        }


        public LeaseStatus getLeaseStatus() {
            return this.leaseStatus;
        }


        public LeaseState getLeaseState() {
            return this.leaseState;
        }


        public LeaseDuration getLeaseDuration() {
            return this.leaseDuration;
        }


        public long getLength() {
            return this.length;
        }


        public Long getPageBlobSequenceNumber() {
            return this.pageBlobSequenceNumber;
        }


        public void setCacheControl(final String cacheControl) {
            this.cacheControl = cacheControl;
        }


        public void setContentDisposition(final String contentDisposition) {
            this.contentDisposition = contentDisposition;
        }


        public void setContentEncoding(final String contentEncoding) {
            this.contentEncoding = contentEncoding;
        }


        public void setContentLanguage(final String contentLanguage) {
            this.contentLanguage = contentLanguage;
        }


        public void setContentMD5(final String contentMD5) {
            this.contentMD5 = contentMD5;
        }


        public void setContentType(final String contentType) {
            this.contentType = contentType;
        }


        protected void setBlobType(final BlobType blobType) {
            this.blobType = blobType;
        }


        protected void setCopyState(final CopyState copyState) {
            this.copyState = copyState;
        }


        protected void setEtag(final String etag) {
            this.etag = etag;
        }


        protected void setLastModified(final Date lastModified) {
            this.lastModified = lastModified;
        }


        protected void setLeaseStatus(final LeaseStatus leaseStatus) {
            this.leaseStatus = leaseStatus;
        }


        protected void setLeaseState(final LeaseState leaseState) {
            this.leaseState = leaseState;
        }


        protected void setLeaseDuration(final LeaseDuration leaseDuration) {
            this.leaseDuration = leaseDuration;
        }


        protected void setLength(final long length) {
            this.length = length;
        }


        protected void setPageBlobSequenceNumber(final Long pageBlobSequenceNumber) {
            this.pageBlobSequenceNumber = pageBlobSequenceNumber;
        }
    }

    public static enum BlockSearchMode {

        COMMITTED,


        UNCOMMITTED,


        LATEST
    }

    static enum LeaseAction {


        ACQUIRE,


        RENEW,


        RELEASE,


        BREAK,


        CHANGE;

        @Override
        public String toString() {
            switch (this) {
                case ACQUIRE:
                    return "Acquire";
                case RENEW:
                    return "Renew";
                case RELEASE:
                    return "Release";
                case BREAK:
                    return "Break";
                case CHANGE:
                    return "Change";
                default:
                    // Wont Happen, all possible values covered above.
                    return Constants.EMPTY_STRING;
            }
        }
    }

    public static interface ListBlobItem {


        CloudBlobContainer getContainer() throws URISyntaxException, StorageException;


        CloudBlobDirectory getParent() throws URISyntaxException, StorageException;


        URI getUri();


        StorageUri getStorageUri();
    }

    static enum PageOperationType {

        UPDATE,


        CLEAR
    }

    public static enum LeaseState {

        UNSPECIFIED,


        AVAILABLE,


        LEASED,


        EXPIRED,


        BREAKING,


        BROKEN;


        protected static LeaseState parse(final String typeString) {
            if (Utility.isNullOrEmpty(typeString)) {
                return UNSPECIFIED;
            }
            else if ("available".equals(typeString.toLowerCase(Locale.US))) {
                return AVAILABLE;
            }
            else if ("leased".equals(typeString.toLowerCase(Locale.US))) {
                return LEASED;
            }
            else if ("expired".equals(typeString.toLowerCase(Locale.US))) {
                return EXPIRED;
            }
            else if ("breaking".equals(typeString.toLowerCase(Locale.US))) {
                return BREAKING;
            }
            else if ("broken".equals(typeString.toLowerCase(Locale.US))) {
                return BROKEN;
            }
            else {
                return UNSPECIFIED;
            }
        }
    }

    static final class BlobContainerAttributes {

        private HashMap<String, String> metadata;


        private BlobContainerProperties properties;


        private String name;


        private StorageUri storageUri;


        public BlobContainerAttributes() {
            this.setMetadata(new HashMap<String, String>());
            this.setProperties(new BlobContainerProperties());
        }


        public HashMap<String, String> getMetadata() {
            return this.metadata;
        }


        public String getName() {
            return this.name;
        }


        public BlobContainerProperties getProperties() {
            return this.properties;
        }


        public final StorageUri getStorageUri() {
            return this.storageUri;
        }


        public URI getUri() {
            return this.storageUri.getPrimaryUri();
        }


        public void setMetadata(final HashMap<String, String> metadata) {
            this.metadata = metadata;
        }


        public void setName(final String name) {
            this.name = name;
        }


        public void setProperties(final BlobContainerProperties properties) {
            this.properties = properties;
        }


        protected void setStorageUri(final StorageUri storageUri) {
            this.storageUri = storageUri;
        }
    }

    public static final class BlobOutputStream extends OutputStream {

        private static Random blockSequenceGenerator = new Random();


        private final CloudBlob parentBlobRef;


        private BlobType streamType = BlobType.UNSPECIFIED;


        volatile boolean streamFaulted;


        Object lastErrorLock = new Object();


        IOException lastError;


        OperationContext opContext;


        BlobRequestOptions options;


        private MessageDigest md5Digest;


        private long blockIdSequenceNumber = -1;


        private ArrayList<BlockEntry> blockList;


        private long currentPageOffset;


        private ByteArrayOutputStream outBuffer;


        private int currentBufferedBytes;


        private int internalWriteThreshold = -1;


        private volatile int outstandingRequests;


        private final ExecutorService threadExecutor;


        private final ExecutorCompletionService<Void> completionService;


        AccessCondition accessCondition = null;


        protected BlobOutputStream(final CloudBlob parentBlob, final AccessCondition accessCondition,
                final BlobRequestOptions options, final OperationContext opContext) throws StorageException {
            this.accessCondition = accessCondition;
            this.parentBlobRef = parentBlob;
            this.parentBlobRef.assertCorrectBlobType();
            this.options = new BlobRequestOptions(options);
            this.outBuffer = new ByteArrayOutputStream();
            this.opContext = opContext;
            this.streamFaulted = false;

            if (this.options.getConcurrentRequestCount() < 1) {
                throw new IllegalArgumentException("ConcurrentRequestCount");
            }

            if (this.options.getStoreBlobContentMD5()) {
                try {
                    this.md5Digest = MessageDigest.getInstance("MD5");
                }
                catch (final NoSuchAlgorithmException e) {
                    // This wont happen, throw fatal.
                    throw Utility.generateNewUnexpectedStorageException(e);
                }
            }

            // V2 cachedThreadPool for perf.
            this.threadExecutor = Executors.newFixedThreadPool(this.options.getConcurrentRequestCount());
            this.completionService = new ExecutorCompletionService<Void>(this.threadExecutor);
        }


        protected BlobOutputStream(final CloudBlockBlob parentBlob, final AccessCondition accessCondition,
                final BlobRequestOptions options, final OperationContext opContext) throws StorageException {
            this((CloudBlob) parentBlob, accessCondition, options, opContext);
            this.blockIdSequenceNumber = (long) (blockSequenceGenerator.nextInt(Integer.MAX_VALUE))
                    + blockSequenceGenerator.nextInt(Integer.MAX_VALUE - 100000);
            this.blockList = new ArrayList<BlockEntry>();

            this.streamType = BlobType.BLOCK_BLOB;
            this.internalWriteThreshold = this.parentBlobRef.getStreamWriteSizeInBytes();
        }


        @DoesServiceRequest
        protected BlobOutputStream(final CloudPageBlob parentBlob, final long length,
                final AccessCondition accessCondition, final BlobRequestOptions options, final OperationContext opContext)
                throws StorageException {
            this(parentBlob, accessCondition, options, opContext);
            this.streamType = BlobType.PAGE_BLOB;
            this.internalWriteThreshold = (int) Math.min(this.parentBlobRef.getStreamWriteSizeInBytes(), length);
        }


        private void checkStreamState() throws IOException {
            synchronized (this.lastErrorLock) {
                if (this.streamFaulted) {
                    throw this.lastError;
                }
            }
        }


        @Override
        @DoesServiceRequest
        public void close() throws IOException {
            try {
                // if the user has already closed the stream, this will throw a STREAM_CLOSED exception
                // if an exception was thrown by any thread in the threadExecutor, realize it now
                this.checkStreamState();

                // flush any remaining data
                this.flush();

                // shut down the ExecutorService.
                this.threadExecutor.shutdown();

                // try to commit the blob
                try {
                    this.commit();
                }
                catch (final StorageException e) {
                    throw Utility.initIOException(e);
                }
            }
            finally {
                // if close() is called again, an exception will be thrown
                synchronized (this.lastErrorLock) {
                    this.streamFaulted = true;
                    this.lastError = new IOException(SR.STREAM_CLOSED);
                }

                // if an exception was thrown and the executor was not yet closed, call shutDownNow() to cancel all tasks
                // and shutdown the ExecutorService
                if (!this.threadExecutor.isShutdown()) {
                    this.threadExecutor.shutdownNow();
                }
            }
        }


        @DoesServiceRequest
        private void commit() throws StorageException {
            if (this.options.getStoreBlobContentMD5()) {
                this.parentBlobRef.getProperties().setContentMD5(Base64.encode(this.md5Digest.digest()));
            }

            if (this.streamType == BlobType.BLOCK_BLOB) {
                // wait for all blocks to finish
                final CloudBlockBlob blobRef = (CloudBlockBlob) this.parentBlobRef;
                blobRef.commitBlockList(this.blockList, this.accessCondition, this.options, this.opContext);
            }
            else if (this.streamType == BlobType.PAGE_BLOB) {
                this.parentBlobRef.uploadProperties(this.accessCondition, this.options, this.opContext);
            }
        }


        @DoesServiceRequest
        private synchronized void dispatchWrite(final int writeLength) throws IOException {
            if (writeLength == 0) {
                return;
            }

            Callable<Void> worker = null;

            if (this.outstandingRequests > this.options.getConcurrentRequestCount() * 2) {
                this.waitForTaskToComplete();
            }

            final ByteArrayInputStream bufferRef = new ByteArrayInputStream(this.outBuffer.toByteArray());

            if (this.streamType == BlobType.BLOCK_BLOB) {
                final CloudBlockBlob blobRef = (CloudBlockBlob) this.parentBlobRef;
                final String blockID = Base64.encode(Utility.getBytesFromLong(this.blockIdSequenceNumber++));
                this.blockList.add(new BlockEntry(blockID, BlockSearchMode.LATEST));

                worker = new Callable<Void>() {
                    @Override
                    public Void call() {
                        try {
                            blobRef.uploadBlock(blockID, bufferRef, writeLength, BlobOutputStream.this.accessCondition,
                                    BlobOutputStream.this.options, BlobOutputStream.this.opContext);
                        }
                        catch (final IOException e) {
                            synchronized (BlobOutputStream.this.lastErrorLock) {
                                BlobOutputStream.this.streamFaulted = true;
                                BlobOutputStream.this.lastError = e;
                            }
                        }
                        catch (final StorageException e) {
                            synchronized (BlobOutputStream.this.lastErrorLock) {
                                BlobOutputStream.this.streamFaulted = true;
                                BlobOutputStream.this.lastError = Utility.initIOException(e);
                            }
                        }
                        return null;
                    }
                };
            }
            else if (this.streamType == BlobType.PAGE_BLOB) {
                final CloudPageBlob blobRef = (CloudPageBlob) this.parentBlobRef;
                long tempOffset = this.currentPageOffset;
                long tempLength = writeLength;

                final long opWriteLength = tempLength;
                final long opOffset = tempOffset;
                this.currentPageOffset += writeLength;

                worker = new Callable<Void>() {
                    @Override
                    public Void call() {
                        try {
                            blobRef.uploadPages(bufferRef, opOffset, opWriteLength, BlobOutputStream.this.accessCondition,
                                    BlobOutputStream.this.options, BlobOutputStream.this.opContext);
                        }
                        catch (final IOException e) {
                            synchronized (BlobOutputStream.this.lastErrorLock) {
                                BlobOutputStream.this.streamFaulted = true;
                                BlobOutputStream.this.lastError = e;
                            }
                        }
                        catch (final StorageException e) {
                            synchronized (BlobOutputStream.this.lastErrorLock) {
                                BlobOutputStream.this.streamFaulted = true;
                                BlobOutputStream.this.lastError = Utility.initIOException(e);
                            }
                        }
                        return null;
                    }
                };
            }

            // Do work and reset buffer.
            this.completionService.submit(worker);
            this.outstandingRequests++;
            this.currentBufferedBytes = 0;
            this.outBuffer = new ByteArrayOutputStream();
        }


        @Override
        @DoesServiceRequest
        public synchronized void flush() throws IOException {
            this.checkStreamState();

            if (this.streamType == BlobType.PAGE_BLOB && this.currentBufferedBytes > 0
                    && (this.currentBufferedBytes % Constants.PAGE_SIZE != 0)) {
                throw new IOException(String.format(SR.INVALID_NUMBER_OF_BYTES_IN_THE_BUFFER, this.currentBufferedBytes));

                // Non 512 byte remainder, uncomment to pad with bytes and commit.

            }

            // Dispatch a write for the current bytes in the buffer
            this.dispatchWrite(this.currentBufferedBytes);

            // Waits for all submitted tasks to complete
            while (this.outstandingRequests > 0) {
                // Wait for a task to complete
                this.waitForTaskToComplete();

                // If that task threw an error, fail fast
                this.checkStreamState();
            }
        }


        private void waitForTaskToComplete() throws IOException {
            try {
                final Future<Void> future = this.completionService.take();
                future.get();
            }
            catch (final InterruptedException e) {
                throw Utility.initIOException(e);
            }
            catch (final ExecutionException e) {
                throw Utility.initIOException(e);
            }

            this.outstandingRequests--;
        }


        @Override
        @DoesServiceRequest
        public void write(final byte[] data) throws IOException {
            this.write(data, 0, data.length);
        }


        @Override
        @DoesServiceRequest
        public void write(final byte[] data, final int offset, final int length) throws IOException {
            if (offset < 0 || length < 0 || length > data.length - offset) {
                throw new IndexOutOfBoundsException();
            }

            this.writeInternal(data, offset, length);
        }


        @DoesServiceRequest
        public void write(final InputStream sourceStream, final long writeLength) throws IOException, StorageException {
            Utility.writeToOutputStream(sourceStream, this, writeLength, false, false, this.opContext, this.options);
        }


        @Override
        @DoesServiceRequest
        public void write(final int byteVal) throws IOException {
            this.write(new byte[] { (byte) (byteVal & 0xFF) });
        }



        @DoesServiceRequest
        private synchronized void writeInternal(final byte[] data, int offset, int length) throws IOException {
            while (length > 0) {
                this.checkStreamState();

                final int availableBufferBytes = this.internalWriteThreshold - this.currentBufferedBytes;
                final int nextWrite = Math.min(availableBufferBytes, length);

                // If we need to set MD5 then update the digest accordingly
                if (this.options.getStoreBlobContentMD5()) {
                    this.md5Digest.update(data, offset, nextWrite);
                }

                this.outBuffer.write(data, offset, nextWrite);
                this.currentBufferedBytes += nextWrite;
                offset += nextWrite;
                length -= nextWrite;

                if (this.currentBufferedBytes == this.internalWriteThreshold) {
                    this.dispatchWrite(this.internalWriteThreshold);
                }
            }
        }
    }

    public static final class BlobInputStream extends InputStream {

        private final CloudBlob parentBlobRef;


        private MessageDigest md5Digest;


        private volatile boolean streamFaulted;


        private IOException lastError;


        private final OperationContext opContext;


        private final BlobRequestOptions options;


        private long streamLength = -1;


        private final int readSize;


        private boolean validateBlobMd5;


        private final String retrievedContentMD5Value;


        private ByteArrayInputStream currentBuffer;


        private long markedPosition;


        private int markExpiry;


        private long currentAbsoluteReadPosition;


        private long bufferStartOffset;


        private int bufferSize;


        private AccessCondition accessCondition = null;


        @DoesServiceRequest
        protected BlobInputStream(final CloudBlob parentBlob, final AccessCondition accessCondition,
                final BlobRequestOptions options, final OperationContext opContext) throws StorageException {
            this.parentBlobRef = parentBlob;
            this.parentBlobRef.assertCorrectBlobType();
            this.options = new BlobRequestOptions(options);
            this.opContext = opContext;
            this.streamFaulted = false;
            this.currentAbsoluteReadPosition = 0;
            this.readSize = parentBlob.getStreamMinimumReadSizeInBytes();

            if (options.getUseTransactionalContentMD5() && this.readSize > 4 * Constants.MB) {
                throw new IllegalArgumentException(SR.INVALID_RANGE_CONTENT_MD5_HEADER);
            }

            parentBlob.downloadAttributes(accessCondition, this.options, this.opContext);

            this.retrievedContentMD5Value = parentBlob.getProperties().getContentMD5();

            // Will validate it if it was returned
            this.validateBlobMd5 = !options.getDisableContentMD5Validation()
                    && !Utility.isNullOrEmpty(this.retrievedContentMD5Value);

            // Validates the first option, and sets future requests to use if match
            // request option.

            // If there is an existing conditional validate it, as we intend to
            // replace if for future requests.
            String previousLeaseId = null;
            if (accessCondition != null) {
                previousLeaseId = accessCondition.getLeaseID();

                if (!accessCondition.verifyConditional(this.parentBlobRef.getProperties().getEtag(), this.parentBlobRef
                        .getProperties().getLastModified())) {
                    throw new StorageException(StorageErrorCode.CONDITION_FAILED.toString(),
                            SR.INVALID_CONDITIONAL_HEADERS, HttpURLConnection.HTTP_PRECON_FAILED, null, null);
                }
            }

            this.accessCondition = AccessCondition.generateIfMatchCondition(this.parentBlobRef.getProperties().getEtag());
            this.accessCondition.setLeaseID(previousLeaseId);

            this.streamLength = parentBlob.getProperties().getLength();

            if (this.validateBlobMd5) {
                try {
                    this.md5Digest = MessageDigest.getInstance("MD5");
                }
                catch (final NoSuchAlgorithmException e) {
                    // This wont happen, throw fatal.
                    throw Utility.generateNewUnexpectedStorageException(e);
                }
            }

            this.reposition(0);
        }


        @Override
        public synchronized int available() throws IOException {
            return this.bufferSize - (int) (this.currentAbsoluteReadPosition - this.bufferStartOffset);
        }


        private synchronized void checkStreamState() throws IOException {
            if (this.streamFaulted) {
                throw this.lastError;
            }
        }


        @Override
        public synchronized void close() throws IOException {
            this.currentBuffer = null;
            this.streamFaulted = true;
            this.lastError = new IOException(SR.STREAM_CLOSED);
        }


        @DoesServiceRequest
        private synchronized void dispatchRead(final int readLength) throws IOException {
            try {
                final byte[] byteBuffer = new byte[readLength];

                this.parentBlobRef.downloadRangeInternal(this.currentAbsoluteReadPosition, (long) readLength, byteBuffer,
                        0, this.accessCondition, this.options, this.opContext);

                this.currentBuffer = new ByteArrayInputStream(byteBuffer);
                this.bufferSize = readLength;
                this.bufferStartOffset = this.currentAbsoluteReadPosition;
            }
            catch (final StorageException e) {
                this.streamFaulted = true;
                this.lastError = Utility.initIOException(e);
                throw this.lastError;
            }
        }


        @Override
        public synchronized void mark(final int readlimit) {
            this.markedPosition = this.currentAbsoluteReadPosition;
            this.markExpiry = readlimit;
        }


        @Override
        public boolean markSupported() {
            return true;
        }


        @Override
        @DoesServiceRequest
        public int read() throws IOException {
            final byte[] tBuff = new byte[1];
            final int numberOfBytesRead = this.read(tBuff, 0, 1);

            if (numberOfBytesRead > 0) {
                return tBuff[0] & 0xFF;
            }
            else if (numberOfBytesRead == 0) {
                throw new IOException(SR.UNEXPECTED_STREAM_READ_ERROR);
            }
            else {
                return -1;
            }
        }


        @Override
        @DoesServiceRequest
        public int read(final byte[] b) throws IOException {
            return this.read(b, 0, b.length);
        }


        @Override
        @DoesServiceRequest
        public int read(final byte[] b, final int off, final int len) throws IOException {
            if (off < 0 || len < 0 || len > b.length - off) {
                throw new IndexOutOfBoundsException();
            }

            return this.readInternal(b, off, len);
        }


        @DoesServiceRequest
        private synchronized int readInternal(final byte[] b, final int off, int len) throws IOException {
            this.checkStreamState();

            // if buffer is empty do next get operation
            if ((this.currentBuffer == null || this.currentBuffer.available() == 0)
                    && this.currentAbsoluteReadPosition < this.streamLength) {
                this.dispatchRead((int) Math.min(this.readSize, this.streamLength - this.currentAbsoluteReadPosition));
            }

            len = Math.min(len, this.readSize);

            // do read from buffer
            final int numberOfBytesRead = this.currentBuffer.read(b, off, len);

            if (numberOfBytesRead > 0) {
                this.currentAbsoluteReadPosition += numberOfBytesRead;

                if (this.validateBlobMd5) {
                    this.md5Digest.update(b, off, numberOfBytesRead);

                    if (this.currentAbsoluteReadPosition == this.streamLength) {
                        // Reached end of stream, validate md5.
                        final String calculatedMd5 = Base64.encode(this.md5Digest.digest());
                        if (!calculatedMd5.equals(this.retrievedContentMD5Value)) {
                            this.lastError = Utility
                                    .initIOException(new StorageException(
                                            StorageErrorCodeStrings.INVALID_MD5,
                                            String.format(
                                                    "Blob data corrupted (integrity check failed), Expected value is %s, retrieved %s",
                                                    this.retrievedContentMD5Value, calculatedMd5),
                                            Constants.HeaderConstants.HTTP_UNUSED_306, null, null));
                            this.streamFaulted = true;
                            throw this.lastError;
                        }
                    }
                }
            }

            // update markers
            if (this.markExpiry > 0 && this.markedPosition + this.markExpiry < this.currentAbsoluteReadPosition) {
                this.markedPosition = 0;
                this.markExpiry = 0;
            }

            return numberOfBytesRead;
        }


        private synchronized void reposition(final long absolutePosition) {
            this.currentAbsoluteReadPosition = absolutePosition;
            this.currentBuffer = new ByteArrayInputStream(new byte[0]);
        }


        @Override
        public synchronized void reset() throws IOException {
            if (this.markedPosition + this.markExpiry < this.currentAbsoluteReadPosition) {
                throw new IOException(SR.MARK_EXPIRED);
            }

            this.validateBlobMd5 = false;
            this.md5Digest = null;
            this.reposition(this.markedPosition);
        }


        @Override
        public synchronized long skip(final long n) throws IOException {
            if (n == 0) {
                return 0;
            }

            if (n < 0 || this.currentAbsoluteReadPosition + n > this.streamLength) {
                throw new IndexOutOfBoundsException();
            }

            this.validateBlobMd5 = false;
            this.md5Digest = null;
            this.reposition(this.currentAbsoluteReadPosition + n);
            return n;
        }
    }

    public static enum BlobListingDetails {

        SNAPSHOTS(1),


        METADATA(2),


        UNCOMMITTED_BLOBS(4),


        COPY(8);


        public int value;


        BlobListingDetails(final int val) {
            this.value = val;
        }
    }

    public static enum LeaseStatus {

        UNSPECIFIED,


        LOCKED,


        UNLOCKED;


        protected static LeaseStatus parse(final String typeString) {
            if (Utility.isNullOrEmpty(typeString)) {
                return UNSPECIFIED;
            }
            else if ("unlocked".equals(typeString.toLowerCase(Locale.US))) {
                return UNLOCKED;
            }
            else if ("locked".equals(typeString.toLowerCase(Locale.US))) {
                return LOCKED;
            }
            else {
                return UNSPECIFIED;
            }
        }
    }

    public static final class CopyState {

        private String copyId;


        private Date completionTime;


        private CopyStatus status;


        private URI source;


        private Long bytesCopied;


        private Long totalBytes;


        private String statusDescription;


        public CopyState() {
        }


        public String getCopyId() {
            return this.copyId;
        }


        public Date getCompletionTime() {
            return this.completionTime;
        }


        public CopyStatus getStatus() {
            return this.status;
        }


        public URI getSource() {
            return this.source;
        }


        public Long getBytesCopied() {
            return this.bytesCopied;
        }


        public Long getTotalBytes() {
            return this.totalBytes;
        }


        public String getStatusDescription() {
            return this.statusDescription;
        }


        protected void setCopyId(final String copyId) {
            this.copyId = copyId;
        }


        protected void setCompletionTime(final Date completionTime) {
            this.completionTime = completionTime;
        }


        protected void setStatus(final CopyStatus status) {
            this.status = status;
        }


        protected void setSource(final URI source) {
            this.source = source;
        }


        protected void setBytesCopied(final Long bytesCopied) {
            this.bytesCopied = bytesCopied;
        }


        protected void setTotalBytes(final Long totalBytes) {
            this.totalBytes = totalBytes;
        }


        protected void setStatusDescription(final String statusDescription) {
            this.statusDescription = statusDescription;
        }
    }

    static final class BlobResponse extends BaseResponse {


        public static String getAcl(final HttpURLConnection request) {
            return request.getHeaderField(BlobConstants.BLOB_PUBLIC_ACCESS_HEADER);
        }


        public static BlobAttributes getBlobAttributes(final HttpURLConnection request, final StorageUri resourceURI,
                final String snapshotID) throws URISyntaxException, ParseException {

            final String blobType = request.getHeaderField(BlobConstants.BLOB_TYPE_HEADER);
            final BlobAttributes attributes = new BlobAttributes(BlobType.parse(blobType));
            final BlobProperties properties = attributes.getProperties();

            properties.setCacheControl(request.getHeaderField(Constants.HeaderConstants.CACHE_CONTROL));
            properties.setContentDisposition(request.getHeaderField(Constants.HeaderConstants.CONTENT_DISPOSITION));
            properties.setContentEncoding(request.getHeaderField(Constants.HeaderConstants.CONTENT_ENCODING));
            properties.setContentLanguage(request.getHeaderField(Constants.HeaderConstants.CONTENT_LANGUAGE));
            properties.setContentMD5(request.getHeaderField(Constants.HeaderConstants.CONTENT_MD5));
            properties.setContentType(request.getHeaderField(Constants.HeaderConstants.CONTENT_TYPE));
            properties.setEtag(BaseResponse.getEtag(request));

            final Calendar lastModifiedCalendar = Calendar.getInstance(Utility.LOCALE_US);
            lastModifiedCalendar.setTimeZone(Utility.UTC_ZONE);
            lastModifiedCalendar.setTime(new Date(request.getLastModified()));
            properties.setLastModified(lastModifiedCalendar.getTime());

            properties.setLeaseStatus(getLeaseStatus(request));
            properties.setLeaseState(getLeaseState(request));
            properties.setLeaseDuration(getLeaseDuration(request));

            final String rangeHeader = request.getHeaderField(Constants.HeaderConstants.CONTENT_RANGE);
            final String xContentLengthHeader = request.getHeaderField(BlobConstants.CONTENT_LENGTH_HEADER);

            if (!Utility.isNullOrEmpty(rangeHeader)) {
                properties.setLength(Long.parseLong(rangeHeader.split("/")[1]));
            }
            else if (!Utility.isNullOrEmpty(xContentLengthHeader)) {
                properties.setLength(Long.parseLong(xContentLengthHeader));
            }
            else {
                // using this instead of the request property since the request
                // property only returns an int.
                final String contentLength = request.getHeaderField(Constants.HeaderConstants.CONTENT_LENGTH);

                if (!Utility.isNullOrEmpty(contentLength)) {
                    properties.setLength(Long.parseLong(contentLength));
                }
            }

            // Get sequence number
            final String sequenceNumber = request.getHeaderField(Constants.HeaderConstants.BLOB_SEQUENCE_NUMBER);
            if (!Utility.isNullOrEmpty(sequenceNumber)) {
                properties.setPageBlobSequenceNumber(Long.parseLong(sequenceNumber));
            }

            attributes.setStorageUri(resourceURI);
            attributes.setSnapshotID(snapshotID);

            attributes.setMetadata(BaseResponse.getMetadata(request));
            properties.setCopyState(getCopyState(request));
            attributes.setProperties(properties);
            return attributes;
        }


        public static BlobContainerAttributes getBlobContainerAttributes(final HttpURLConnection request,
                final boolean usePathStyleUris) throws StorageException {
            final BlobContainerAttributes containerAttributes = new BlobContainerAttributes();
            URI tempURI;
            try {
                tempURI = PathUtility.stripSingleURIQueryAndFragment(request.getURL().toURI());
            }
            catch (final URISyntaxException e) {
                final StorageException wrappedUnexpectedException = Utility.generateNewUnexpectedStorageException(e);
                throw wrappedUnexpectedException;
            }

            containerAttributes.setName(PathUtility.getContainerNameFromUri(tempURI, usePathStyleUris));

            final BlobContainerProperties containerProperties = containerAttributes.getProperties();
            containerProperties.setEtag(BaseResponse.getEtag(request));
            containerProperties.setLastModified(new Date(request.getLastModified()));
            containerAttributes.setMetadata(getMetadata(request));

            containerProperties.setLeaseStatus(getLeaseStatus(request));
            containerProperties.setLeaseState(getLeaseState(request));
            containerProperties.setLeaseDuration(getLeaseDuration(request));

            return containerAttributes;
        }


        public static CopyState getCopyState(final HttpURLConnection request) throws URISyntaxException, ParseException {
            String copyStatusString = request.getHeaderField(Constants.HeaderConstants.COPY_STATUS);
            if (!Utility.isNullOrEmpty(copyStatusString)) {
                CopyState copyState = new CopyState();
                copyState.setStatus(CopyStatus.parse(copyStatusString));
                copyState.setCopyId(request.getHeaderField(Constants.HeaderConstants.COPY_ID));
                copyState.setStatusDescription(request.getHeaderField(Constants.HeaderConstants.COPY_STATUS_DESCRIPTION));

                final String copyProgressString = request.getHeaderField(Constants.HeaderConstants.COPY_PROGRESS);
                if (!Utility.isNullOrEmpty(copyProgressString)) {
                    String[] progressSequence = copyProgressString.split("/");
                    copyState.setBytesCopied(Long.parseLong(progressSequence[0]));
                    copyState.setTotalBytes(Long.parseLong(progressSequence[1]));
                }

                final String copySourceString = request.getHeaderField(Constants.HeaderConstants.COPY_SOURCE);
                if (!Utility.isNullOrEmpty(copySourceString)) {
                    copyState.setSource(new URI(copySourceString));
                }

                final String copyCompletionTimeString = request
                        .getHeaderField(Constants.HeaderConstants.COPY_COMPLETION_TIME);
                if (!Utility.isNullOrEmpty(copyCompletionTimeString)) {
                    copyState.setCompletionTime(Utility.parseRFC1123DateFromStringInGMT(copyCompletionTimeString));
                }

                return copyState;
            }
            else {
                return null;
            }
        }


        public static LeaseDuration getLeaseDuration(final HttpURLConnection request) {
            final String leaseDuration = request.getHeaderField(Constants.HeaderConstants.LEASE_DURATION);
            if (!Utility.isNullOrEmpty(leaseDuration)) {
                return LeaseDuration.parse(leaseDuration);
            }

            return LeaseDuration.UNSPECIFIED;
        }


        public static String getLeaseID(final HttpURLConnection request) {
            return request.getHeaderField(Constants.HeaderConstants.LEASE_ID_HEADER);
        }


        public static LeaseState getLeaseState(final HttpURLConnection request) {
            final String leaseState = request.getHeaderField(Constants.HeaderConstants.LEASE_STATE);
            if (!Utility.isNullOrEmpty(leaseState)) {
                return LeaseState.parse(leaseState);
            }

            return LeaseState.UNSPECIFIED;
        }


        public static LeaseStatus getLeaseStatus(final HttpURLConnection request) {
            final String leaseStatus = request.getHeaderField(Constants.HeaderConstants.LEASE_STATUS);
            if (!Utility.isNullOrEmpty(leaseStatus)) {
                return LeaseStatus.parse(leaseStatus);
            }

            return LeaseStatus.UNSPECIFIED;
        }


        public static String getLeaseTime(final HttpURLConnection request) {
            return request.getHeaderField(Constants.HeaderConstants.LEASE_TIME_HEADER);
        }


        public static String getSnapshotTime(final HttpURLConnection request) {
            return request.getHeaderField(Constants.HeaderConstants.SNAPSHOT_ID_HEADER);
        }
    }

    public static enum BlockListingFilter {

        COMMITTED,


        UNCOMMITTED,


        ALL
    }

    static final class BlobListingContext extends ListingContext {


        private String delimiter;


        private EnumSet<BlobListingDetails> listingDetails;


        BlobListingContext(final String prefix, final Integer maxResults, final String delimiter,
                final EnumSet<BlobListingDetails> listingDetails) {
            super(prefix, maxResults);
            this.setDelimiter(delimiter);
            this.setListingDetails(listingDetails);
        }


        String getDelimiter() {
            return this.delimiter;
        }


        EnumSet<BlobListingDetails> getListingDetails() {
            return this.listingDetails;
        }


        void setDelimiter(final String delimiter) {
            this.delimiter = delimiter;
        }


        void setListingDetails(final EnumSet<BlobListingDetails> listingDetails) {
            this.listingDetails = listingDetails;
        }
    }

    static final class BlobAttributes {


        private HashMap<String, String> metadata;


        private BlobProperties properties;


        private String snapshotID;


        private StorageUri storageUri;


        public BlobAttributes(final BlobType type) {
            this.setMetadata(new HashMap<String, String>());
            this.setProperties(new BlobProperties(type));
        }


        public HashMap<String, String> getMetadata() {
            return this.metadata;
        }


        public BlobProperties getProperties() {
            return this.properties;
        }


        public final String getSnapshotID() {
            return this.snapshotID;
        }


        public final StorageUri getStorageUri() {
            return this.storageUri;
        }


        public final URI getUri() {
            return this.storageUri.getPrimaryUri();
        }


        protected void setMetadata(final HashMap<String, String> metadata) {
            this.metadata = metadata;
        }


        protected void setProperties(final BlobProperties properties) {
            this.properties = properties;
        }


        protected final void setSnapshotID(String snapshotID) {
            this.snapshotID = snapshotID;
        }


        protected void setStorageUri(final StorageUri storageUri) {
            this.storageUri = storageUri;
        }
    }

    public static final class CloudBlobDirectory implements ListBlobItem {


        private final CloudBlobContainer container;


        private CloudBlobDirectory parent;


        private final CloudBlobClient blobServiceClient;


        private final StorageUri storageUri;


        private final String prefix;


        protected CloudBlobDirectory(final StorageUri uri, final String prefix, final CloudBlobClient client,
                final CloudBlobContainer container) {
            Utility.assertNotNull("uri", uri);
            Utility.assertNotNull("client", client);
            Utility.assertNotNull("container", container);

            this.blobServiceClient = client;
            this.container = container;
            this.prefix = prefix;
            this.storageUri = uri;
        }


        protected CloudBlobDirectory(final StorageUri uri, final String prefix, final CloudBlobClient client,
                final CloudBlobContainer container, final CloudBlobDirectory parent) {
            Utility.assertNotNull("uri", uri);
            Utility.assertNotNull("client", client);
            Utility.assertNotNull("container", container);

            this.blobServiceClient = client;
            this.parent = parent;
            this.container = container;
            this.prefix = prefix;
            this.storageUri = uri;
        }


        public CloudBlockBlob getBlockBlobReference(final String blobName) throws URISyntaxException, StorageException {
            return this.getBlockBlobReference(blobName, null);
        }


        public CloudBlockBlob getBlockBlobReference(final String blobName, final String snapshotID)
                throws URISyntaxException, StorageException {
            Utility.assertNotNullOrEmpty("blobName", blobName);

            final StorageUri address = PathUtility.appendPathToUri(this.storageUri, blobName,
                    this.blobServiceClient.getDirectoryDelimiter());

            final CloudBlockBlob retBlob = new CloudBlockBlob(address, snapshotID, this.blobServiceClient);
            retBlob.setContainer(this.container);
            return retBlob;
        }


        @Override
        public CloudBlobContainer getContainer() throws StorageException, URISyntaxException {
            return this.container;
        }


        public CloudPageBlob getPageBlobReference(final String blobName) throws URISyntaxException, StorageException {
            return this.getPageBlobReference(blobName, null);
        }


        public CloudPageBlob getPageBlobReference(final String blobName, final String snapshotID)
                throws URISyntaxException, StorageException {
            Utility.assertNotNullOrEmpty("blobName", blobName);

            final StorageUri address = PathUtility.appendPathToUri(this.storageUri, blobName,
                    this.blobServiceClient.getDirectoryDelimiter());

            final CloudPageBlob retBlob = new CloudPageBlob(address, snapshotID, this.blobServiceClient);
            retBlob.setContainer(this.container);

            return retBlob;
        }


        @Override
        public CloudBlobDirectory getParent() throws URISyntaxException, StorageException {
            if (this.parent == null) {
                final String parentName = CloudBlob.getParentNameFromURI(this.getStorageUri(),
                        this.blobServiceClient.getDirectoryDelimiter(), this.getContainer());

                if (parentName != null) {
                    StorageUri parentURI = PathUtility.appendPathToUri(this.container.getStorageUri(), parentName);
                    this.parent = new CloudBlobDirectory(parentURI, parentName, this.blobServiceClient, this.getContainer());
                }
            }
            return this.parent;
        }


        public String getPrefix() {
            return this.prefix;
        }


        public CloudBlobClient getServiceClient() {
            return this.blobServiceClient;
        }


        public CloudBlobDirectory getDirectoryReference(String directoryName) throws URISyntaxException {
            Utility.assertNotNullOrEmpty("directoryName", directoryName);

            if (!directoryName.endsWith(this.blobServiceClient.getDirectoryDelimiter())) {
                directoryName = directoryName.concat(this.blobServiceClient.getDirectoryDelimiter());
            }
            final String subDirName = this.getPrefix().concat(directoryName);

            final StorageUri address = PathUtility.appendPathToUri(this.storageUri, directoryName,
                    this.blobServiceClient.getDirectoryDelimiter());

            return new CloudBlobDirectory(address, subDirName, this.blobServiceClient, this.container, this);
        }


        @Deprecated
        public CloudBlobDirectory getSubDirectoryReference(String directoryName) throws URISyntaxException {
            return this.getDirectoryReference(directoryName);
        }


        @Override
        public URI getUri() {
            return this.storageUri.getPrimaryUri();
        }


        @Override
        public final StorageUri getStorageUri() {
            return this.storageUri;
        }


        @DoesServiceRequest
        public Iterable<ListBlobItem> listBlobs() throws StorageException, URISyntaxException {
            return this.getContainer().listBlobs(this.getPrefix());
        }


        @DoesServiceRequest
        public Iterable<ListBlobItem> listBlobs(String prefix) throws URISyntaxException, StorageException {
            prefix = prefix == null ? Constants.EMPTY_STRING : prefix;
            return this.getContainer().listBlobs(this.getPrefix().concat(prefix));
        }


        @DoesServiceRequest
        public Iterable<ListBlobItem> listBlobs(String prefix, final boolean useFlatBlobListing,
                final EnumSet<BlobListingDetails> listingDetails, final BlobRequestOptions options,
                final OperationContext opContext) throws URISyntaxException, StorageException {
            prefix = prefix == null ? Constants.EMPTY_STRING : prefix;
            return this.getContainer().listBlobs(this.getPrefix().concat(prefix), useFlatBlobListing, listingDetails,
                    options, opContext);
        }


        @DoesServiceRequest
        public ResultSegment<ListBlobItem> listBlobsSegmented() throws StorageException, URISyntaxException {
            return this.getContainer().listBlobsSegmented(this.getPrefix());
        }


        @DoesServiceRequest
        public ResultSegment<ListBlobItem> listBlobsSegmented(String prefix) throws StorageException, URISyntaxException {
            prefix = prefix == null ? Constants.EMPTY_STRING : prefix;
            return this.getContainer().listBlobsSegmented(this.getPrefix().concat(prefix));
        }


        @DoesServiceRequest
        public ResultSegment<ListBlobItem> listBlobsSegmented(String prefix, final boolean useFlatBlobListing,
                final EnumSet<BlobListingDetails> listingDetails, final int maxResults,
                final ResultContinuation continuationToken, final BlobRequestOptions options,
                final OperationContext opContext) throws StorageException, URISyntaxException {
            prefix = prefix == null ? Constants.EMPTY_STRING : prefix;
            return this.getContainer().listBlobsSegmented(this.getPrefix().concat(prefix), useFlatBlobListing,
                    listingDetails, maxResults, continuationToken, options, opContext);
        }
    }

    public static final class SharedAccessBlobHeaders {


        private String cacheControl;


        private String contentDisposition;


        private String contentEncoding;


        private String contentLanguage;


        private String contentType;


        public SharedAccessBlobHeaders() {

        }


        public SharedAccessBlobHeaders(SharedAccessBlobHeaders other) {
            Utility.assertNotNull("other", other);

            this.contentType = other.contentType;
            this.contentDisposition = other.contentDisposition;
            this.contentEncoding = other.contentEncoding;
            this.contentLanguage = other.contentLanguage;
            this.cacheControl = other.cacheControl;
        }


        public final String getCacheControl() {
            return this.cacheControl;
        }


        public void setCacheControl(String cacheControl) {
            this.cacheControl = cacheControl;
        }


        public final String getContentDisposition() {
            return this.contentDisposition;
        }


        public void setContentDisposition(String contentDisposition) {
            this.contentDisposition = contentDisposition;
        }


        public final String getContentEncoding() {
            return this.contentEncoding;
        }


        public void setContentEncoding(String contentEncoding) {
            this.contentEncoding = contentEncoding;
        }


        public final String getContentLanguage() {
            return this.contentLanguage;
        }


        public void setContentLanguage(String contentLanguage) {
            this.contentLanguage = contentLanguage;
        }


        public final String getContentType() {
            return this.contentType;
        }


        public void setContentType(String contentType) {
            this.contentType = contentType;
        }
    }

    public static final class BlockEntry {

        private String id;


        private long size;


        private BlockSearchMode searchMode;


        public BlockEntry(final String id) {
            this.setId(id);
            this.searchMode = BlockSearchMode.LATEST;
        }


        public BlockEntry(final String id, final BlockSearchMode searchMode) {
            this.setId(id);
            this.searchMode = searchMode;
        }


        public String getId() {
            return this.id;
        }


        public long getSize() {
            return this.size;
        }


        public BlockSearchMode getSearchMode() {
            return this.searchMode;
        }


        public void setId(final String id) {
            this.id = id;
        }


        public void setSize(final long size) {
            this.size = size;
        }


        public void setSearchMode(BlockSearchMode searchMode) {
            this.searchMode = searchMode;
        }
    }

    static final class BlobConstants {

        public static final String AUTHENTICATION_ERROR_DETAIL = "AuthenticationErrorDetail";


        public static final String BLOB_CONTENT_MD5_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER + "blob-content-md5";


        public static final String BLOB_ELEMENT = "Blob";


        public static final String BLOB_PREFIX_ELEMENT = "BlobPrefix";


        public static final String BLOB_PUBLIC_ACCESS_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER + "blob-public-access";


        public static final String BLOB_TYPE_ELEMENT = "BlobType";

        public static final String BLOB_TYPE_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER + "blob-type";


        public static final String BLOBS_ELEMENT = "Blobs";


        public static final String BLOCK_BLOB = "BlockBlob";


        public static final String BLOCK_BLOB_VALUE = "BlockBlob";


        public static final String BLOCK_ELEMENT = "Block";


        public static final String BLOCK_LIST_ELEMENT = "BlockList";


        public static final String COMMITTED_BLOCKS_ELEMENT = "CommittedBlocks";


        public static final String COMMITTED_ELEMENT = "Committed";


        public static final String CONTAINER_ELEMENT = "Container";


        public static final String CONTAINERS_ELEMENT = "Containers";


        public static final String CONTENT_DISPOSITION_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER
                + "blob-content-disposition";


        public static final String CONTENT_ENCODING_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER + "blob-content-encoding";


        public static final String CONTENT_LANGUAGE_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER + "blob-content-language";


        public static final String CONTENT_LENGTH_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER + "blob-content-length";


        public static final String CONTENT_TYPE_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER + "blob-content-type";


        public static final int DEFAULT_CONCURRENT_REQUEST_COUNT = 1;


        public static final int DEFAULT_COPY_TIMEOUT_IN_SECONDS = 3600;


        public static final String DEFAULT_DELIMITER = "/";


        public static final int DEFAULT_POLLING_INTERVAL_IN_SECONDS = 30;


        public static final int DEFAULT_SINGLE_BLOB_PUT_THRESHOLD_IN_BYTES = 32 * Constants.MB;


        public static final String INCLUDE_SNAPSHOTS_VALUE = "include";


        public static final String LATEST_ELEMENT = "Latest";


        // Note if this is updated then Constants.MAX_MARK_LENGTH needs to be as well.
        public static final int MAX_SINGLE_UPLOAD_BLOB_SIZE_IN_BYTES = 64 * Constants.MB;


        public static final String PAGE_BLOB = "PageBlob";


        public static final String PAGE_BLOB_VALUE = "PageBlob";


        public static final String PAGE_LIST_ELEMENT = "PageList";


        public static final String PAGE_RANGE_ELEMENT = "PageRange";


        public static final String PAGE_WRITE = Constants.PREFIX_FOR_STORAGE_HEADER + "page-write";


        public static final String SEQUENCE_NUMBER = Constants.PREFIX_FOR_STORAGE_HEADER + "blob-sequence-number";


        public static final String SIZE = Constants.PREFIX_FOR_STORAGE_HEADER + "blob-content-length";


        public static final String SIZE_ELEMENT = "Size";


        public static final String SNAPSHOT = "snapshot";


        public static final String SNAPSHOT_ELEMENT = "Snapshot";


        public static final String SNAPSHOT_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER + "snapshot";


        public static final String SNAPSHOTS_ONLY_VALUE = "only";


        public static final String START_ELEMENT = "Start";


        public static final String UNCOMMITTED_BLOCKS_ELEMENT = "UncommittedBlocks";


        public static final String UNCOMMITTED_ELEMENT = "Uncommitted";


        public static final String LEASE = "lease";


        private BlobConstants() {
            // No op
        }
    }

    public abstract static class CloudBlob implements ListBlobItem {

        HashMap<String, String> metadata;


        BlobProperties properties;


        private StorageUri storageUri;


        String snapshotID;


        private CloudBlobContainer container;


        protected CloudBlobDirectory parent;


        private String name;


        protected int streamWriteSizeInBytes = Constants.DEFAULT_STREAM_WRITE_IN_BYTES;


        protected int streamMinimumReadSizeInBytes = Constants.DEFAULT_MINIMUM_READ_SIZE_IN_BYTES;


        protected CloudBlobClient blobServiceClient;


        protected CloudBlob(final BlobType type) {
            this.metadata = new HashMap<String, String>();
            this.properties = new BlobProperties(type);
        }


        protected CloudBlob(final BlobType type, final StorageUri uri, final CloudBlobClient client)
                throws StorageException {
            this(type);

            Utility.assertNotNull("blobAbsoluteUri", uri);

            this.blobServiceClient = client;
            this.storageUri = uri;

            this.parseURIQueryStringAndVerify(
                    this.storageUri,
                    client,
                    client == null ? Utility.determinePathStyleFromUri(this.storageUri.getPrimaryUri()) : client
                            .isUsePathStyleUris());
        }


        protected CloudBlob(final BlobType type, final StorageUri uri, final CloudBlobClient client,
                final CloudBlobContainer container) throws StorageException {
            this(type, uri, client);
            this.container = container;
        }


        protected CloudBlob(final BlobType type, final StorageUri uri, final String snapshotID, final CloudBlobClient client)
                throws StorageException {
            this(type, uri, client);
            if (snapshotID != null) {
                if (this.snapshotID != null) {
                    throw new IllegalArgumentException(SR.SNAPSHOT_QUERY_OPTION_ALREADY_DEFINED);
                }
                else {
                    this.snapshotID = snapshotID;
                }
            }
        }


        protected CloudBlob(final CloudBlob otherBlob) {
            this.metadata = new HashMap<String, String>();
            this.properties = new BlobProperties(otherBlob.properties);

            if (otherBlob.metadata != null) {
                this.metadata = new HashMap<String, String>();
                for (final String key : otherBlob.metadata.keySet()) {
                    this.metadata.put(key, otherBlob.metadata.get(key));
                }
            }

            this.snapshotID = otherBlob.snapshotID;
            this.storageUri = otherBlob.storageUri;
            this.container = otherBlob.container;
            this.parent = otherBlob.parent;
            this.blobServiceClient = otherBlob.blobServiceClient;
            this.name = otherBlob.name;
            this.setStreamMinimumReadSizeInBytes(otherBlob.getStreamMinimumReadSizeInBytes());
            this.setStreamWriteSizeInBytes(otherBlob.getStreamWriteSizeInBytes());
        }


        @DoesServiceRequest
        public final void abortCopy(final String copyId) throws StorageException {
            this.abortCopy(copyId, null , null , null );
        }


        @DoesServiceRequest
        public final void abortCopy(final String copyId, final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.abortCopyImpl(copyId, accessCondition, options), options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, Void> abortCopyImpl(final String copyId,
                final AccessCondition accessCondition, final BlobRequestOptions options) {
            Utility.assertNotNull("copyId", copyId);

            final StorageRequest<CloudBlobClient, CloudBlob, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, Void>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    return BlobRequest.abortCopy(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                            options, context, accessCondition, copyId);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0, null);
                }

                @Override
                public Void preProcessResponse(CloudBlob parentObject, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_ACCEPTED) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    return null;
                }
            };

            return putRequest;
        }


        @DoesServiceRequest
        public final String acquireLease(final Integer leaseTimeInSeconds, final String proposedLeaseId)
                throws StorageException {
            return this.acquireLease(leaseTimeInSeconds, proposedLeaseId, null , null ,
                    null );
        }


        @DoesServiceRequest
        public final String acquireLease(final Integer leaseTimeInSeconds, final String proposedLeaseId,
                final AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
                throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.acquireLeaseImpl(leaseTimeInSeconds, proposedLeaseId, accessCondition, options),
                    options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, String> acquireLeaseImpl(final Integer leaseTimeInSeconds,
                final String proposedLeaseId, final AccessCondition accessCondition, final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlob, String> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, String>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    return BlobRequest.leaseBlob(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                            options, context, accessCondition, LeaseAction.ACQUIRE, leaseTimeInSeconds, proposedLeaseId,
                            null);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);
                }

                @Override
                public String preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_CREATED) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    updateEtagAndLastModifiedFromResponse(this.getConnection());
                    blob.properties.setLeaseStatus(LeaseStatus.LOCKED);

                    return BlobResponse.getLeaseID(this.getConnection());
                }
            };

            return putRequest;
        }


        protected final void assertCorrectBlobType() throws StorageException {
            if (this instanceof CloudBlockBlob && this.properties.getBlobType() != BlobType.BLOCK_BLOB) {
                throw new StorageException(StorageErrorCodeStrings.INCORRECT_BLOB_TYPE, String.format(SR.INVALID_BLOB_TYPE,
                        BlobType.BLOCK_BLOB, this.properties.getBlobType()), Constants.HeaderConstants.HTTP_UNUSED_306,
                        null, null);
            }
            if (this instanceof CloudPageBlob && this.properties.getBlobType() != BlobType.PAGE_BLOB) {
                throw new StorageException(StorageErrorCodeStrings.INCORRECT_BLOB_TYPE, String.format(SR.INVALID_BLOB_TYPE,
                        BlobType.PAGE_BLOB, this.properties.getBlobType()), Constants.HeaderConstants.HTTP_UNUSED_306,
                        null, null);

            }
        }


        protected void assertNoWriteOperationForSnapshot() {
            if (isSnapshot()) {
                throw new IllegalArgumentException(SR.INVALID_OPERATION_FOR_A_SNAPSHOT);
            }
        }


        @DoesServiceRequest
        public final long breakLease(final Integer breakPeriodInSeconds) throws StorageException {
            return this
                    .breakLease(breakPeriodInSeconds, null , null , null );
        }


        @DoesServiceRequest
        public final long breakLease(final Integer breakPeriodInSeconds, final AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            if (breakPeriodInSeconds != null) {
                Utility.assertGreaterThanOrEqual("breakPeriodInSeconds", breakPeriodInSeconds, 0);
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.breakLeaseImpl(breakPeriodInSeconds, accessCondition, options), options.getRetryPolicyFactory(),
                    opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, Long> breakLeaseImpl(final Integer breakPeriodInSeconds,
                final AccessCondition accessCondition, final BlobRequestOptions options) {

            final StorageRequest<CloudBlobClient, CloudBlob, Long> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, Long>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    return BlobRequest.leaseBlob(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                            options, context, accessCondition, LeaseAction.BREAK, null, null, breakPeriodInSeconds);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);
                }

                @Override
                public Long preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_ACCEPTED) {
                        this.setNonExceptionedRetryableFailure(true);
                        return -1L;
                    }

                    updateEtagAndLastModifiedFromResponse(this.getConnection());

                    final String leaseTime = BlobResponse.getLeaseTime(this.getConnection());

                    blob.properties.setLeaseStatus(LeaseStatus.UNLOCKED);
                    return Utility.isNullOrEmpty(leaseTime) ? -1L : Long.parseLong(leaseTime);
                }
            };

            return putRequest;
        }


        @DoesServiceRequest
        public final String changeLease(final String proposedLeaseId, final AccessCondition accessCondition)
                throws StorageException {
            return this.changeLease(proposedLeaseId, accessCondition, null , null );
        }


        @DoesServiceRequest
        public final String changeLease(final String proposedLeaseId, final AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException {
            Utility.assertNotNull("accessCondition", accessCondition);
            Utility.assertNotNullOrEmpty("leaseID", accessCondition.getLeaseID());

            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.changeLeaseImpl(proposedLeaseId, accessCondition, options), options.getRetryPolicyFactory(),
                    opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, String> changeLeaseImpl(final String proposedLeaseId,
                final AccessCondition accessCondition, final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlob, String> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, String>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    return BlobRequest.leaseBlob(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                            options, context, accessCondition, LeaseAction.CHANGE, null, proposedLeaseId, null);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);
                }

                @Override
                public String preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    updateEtagAndLastModifiedFromResponse(this.getConnection());

                    return BlobResponse.getLeaseID(this.getConnection());
                }
            };

            return putRequest;
        }


        @DoesServiceRequest
        public final String startCopyFromBlob(final CloudBlob sourceBlob) throws StorageException, URISyntaxException {
            return this.startCopyFromBlob(sourceBlob, null ,
                    null , null , null );
        }


        @DoesServiceRequest
        public final String startCopyFromBlob(final CloudBlob sourceBlob, final AccessCondition sourceAccessCondition,
                final AccessCondition destinationAccessCondition, BlobRequestOptions options, OperationContext opContext)
                throws StorageException, URISyntaxException {
            Utility.assertNotNull("sourceBlob", sourceBlob);
            return this.startCopyFromBlob(
                    sourceBlob.getServiceClient().getCredentials().transformUri(sourceBlob.getQualifiedUri()),
                    sourceAccessCondition, destinationAccessCondition, options, opContext);

        }


        @DoesServiceRequest
        public final String startCopyFromBlob(final URI source) throws StorageException {
            return this.startCopyFromBlob(source, null ,
                    null , null , null );
        }


        @DoesServiceRequest
        public final String startCopyFromBlob(final URI source, final AccessCondition sourceAccessCondition,
                final AccessCondition destinationAccessCondition, BlobRequestOptions options, OperationContext opContext)
                throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.startCopyFromBlobImpl(source, sourceAccessCondition, destinationAccessCondition, options),
                    options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, String> startCopyFromBlobImpl(final URI source,
                final AccessCondition sourceAccessCondition, final AccessCondition destinationAccessCondition,
                final BlobRequestOptions options) {

            final StorageRequest<CloudBlobClient, CloudBlob, String> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, String>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    // toASCIIString() must be used in order to appropriately encode the URI
                    return BlobRequest.copyFrom(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                            options, context, sourceAccessCondition, destinationAccessCondition, source.toASCIIString(),
                            blob.snapshotID);
                }

                @Override
                public void setHeaders(HttpURLConnection connection, CloudBlob blob, OperationContext context) {
                    BlobRequest.addMetadata(connection, blob.metadata, context);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0, null);
                }

                @Override
                public String preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_ACCEPTED) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    blob.updateEtagAndLastModifiedFromResponse(this.getConnection());
                    blob.properties.setCopyState(BlobResponse.getCopyState(this.getConnection()));

                    return blob.properties.getCopyState().getCopyId();
                }
            };

            return putRequest;
        }


        @DoesServiceRequest
        public final CloudBlob createSnapshot() throws StorageException {
            return this
                    .createSnapshot(null , null , null , null );
        }


        @DoesServiceRequest
        public final CloudBlob createSnapshot(final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            return this.createSnapshot(null , accessCondition, options, opContext);
        }


        @DoesServiceRequest
        public final CloudBlob createSnapshot(final HashMap<String, String> metadata,
                final AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
                throws StorageException {
            assertNoWriteOperationForSnapshot();

            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            return ExecutionEngine
                    .executeWithRetry(this.blobServiceClient, this,
                            this.createSnapshotImpl(metadata, accessCondition, options), options.getRetryPolicyFactory(),
                            opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, CloudBlob> createSnapshotImpl(
                final HashMap<String, String> metadata, final AccessCondition accessCondition,
                final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlob, CloudBlob> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, CloudBlob>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    return BlobRequest.snapshot(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                            options, context, accessCondition);
                }

                @Override
                public void setHeaders(HttpURLConnection connection, CloudBlob blob, OperationContext context) {
                    if (metadata != null) {
                        BlobRequest.addMetadata(connection, metadata, context);
                    }
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);
                }

                @Override
                public CloudBlob preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_CREATED) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }
                    CloudBlob snapshot = null;
                    final String snapshotTime = BlobResponse.getSnapshotTime(this.getConnection());
                    if (blob instanceof CloudBlockBlob) {
                        snapshot = new CloudBlockBlob(blob.getStorageUri(), snapshotTime, client);
                    }
                    else if (blob instanceof CloudPageBlob) {
                        snapshot = new CloudPageBlob(blob.getStorageUri(), snapshotTime, client);
                    }
                    snapshot.setProperties(blob.properties);

                    // use the specified metadata if not null : otherwise blob's metadata
                    snapshot.setMetadata(metadata != null ? metadata : blob.metadata);

                    snapshot.updateEtagAndLastModifiedFromResponse(this.getConnection());

                    return snapshot;
                }
            };

            return putRequest;
        }


        @DoesServiceRequest
        public final void delete() throws StorageException {
            this.delete(DeleteSnapshotsOption.NONE, null , null , null );
        }


        @DoesServiceRequest
        public final void delete(final DeleteSnapshotsOption deleteSnapshotsOption, final AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException {
            Utility.assertNotNull("deleteSnapshotsOption", deleteSnapshotsOption);

            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.deleteImpl(deleteSnapshotsOption, accessCondition, options), options.getRetryPolicyFactory(),
                    opContext);
        }


        @DoesServiceRequest
        public final boolean deleteIfExists() throws StorageException {
            return this
                    .deleteIfExists(DeleteSnapshotsOption.NONE, null , null , null );
        }


        @DoesServiceRequest
        public final boolean deleteIfExists(final DeleteSnapshotsOption deleteSnapshotsOption,
                final AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
                throws StorageException {
            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            boolean exists = this.exists(true, accessCondition, options, opContext);
            if (exists) {
                try {
                    this.delete(deleteSnapshotsOption, accessCondition, options, opContext);
                    return true;
                }
                catch (StorageException e) {
                    if (e.getHttpStatusCode() == HttpURLConnection.HTTP_NOT_FOUND
                            && StorageErrorCodeStrings.BLOB_NOT_FOUND.equals(e.getErrorCode())) {
                        return false;
                    }
                    else {
                        throw e;
                    }
                }

            }
            else {
                return false;
            }
        }

        private StorageRequest<CloudBlobClient, CloudBlob, Void> deleteImpl(
                final DeleteSnapshotsOption deleteSnapshotsOption, final AccessCondition accessCondition,
                final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlob, Void> deleteRequest = new StorageRequest<CloudBlobClient, CloudBlob, Void>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    return BlobRequest.deleteBlob(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                            options, context, accessCondition, blob.snapshotID, deleteSnapshotsOption);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, -1L, null);
                }

                @Override
                public Void preProcessResponse(CloudBlob parentObject, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_ACCEPTED) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    return null;
                }
            };

            return deleteRequest;
        }


        @DoesServiceRequest
        public final void download(final OutputStream outStream) throws StorageException {
            this.download(outStream, null , null , null );
        }


        @DoesServiceRequest
        public final void download(final OutputStream outStream, final AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this, this.downloadToStreamImpl(
                    null, null, outStream, accessCondition, options, opContext), options
                    .getRetryPolicyFactory(), opContext);
        }


        @DoesServiceRequest
        public final void downloadRange(final long offset, final Long length, final OutputStream outStream)
                throws StorageException {
            this.downloadRange(offset, length, outStream, null , null , null );
        }


        @DoesServiceRequest
        public final void downloadRange(final long offset, final Long length, final OutputStream outStream,
                final AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
                throws StorageException {
            if (offset < 0 || (length != null && length <= 0)) {
                throw new IndexOutOfBoundsException();
            }

            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            if (options.getUseTransactionalContentMD5() && (length != null && length > 4 * Constants.MB)) {
                throw new IllegalArgumentException(SR.INVALID_RANGE_CONTENT_MD5_HEADER);
            }

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.downloadToStreamImpl(offset, length, outStream, accessCondition, options, opContext),
                    options.getRetryPolicyFactory(), opContext);
        }


        @DoesServiceRequest
        public final void downloadAttributes() throws StorageException {
            this.downloadAttributes(null , null , null );
        }


        @DoesServiceRequest
        public final void downloadAttributes(final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.downloadAttributesImpl(accessCondition, options), options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, Void> downloadAttributesImpl(
                final AccessCondition accessCondition, final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlob, Void> getRequest = new StorageRequest<CloudBlobClient, CloudBlob, Void>(
                    options, this.getStorageUri()) {

                @Override
                public void setRequestLocationMode() {
                    this.setRequestLocationMode(RequestLocationMode.PRIMARY_OR_SECONDARY);
                }

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    return BlobRequest.getBlobProperties(
                            blob.getTransformedAddress(context).getUri(this.getCurrentLocation()), options, context,
                            accessCondition, blob.snapshotID);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, -1L, null);
                }

                @Override
                public Void preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    // Set attributes
                    final BlobAttributes retrievedAttributes = BlobResponse.getBlobAttributes(this.getConnection(),
                            blob.getStorageUri(), blob.snapshotID);

                    if (retrievedAttributes.getProperties().getBlobType() != blob.properties.getBlobType()) {
                        throw new StorageException(StorageErrorCodeStrings.INCORRECT_BLOB_TYPE, String.format(
                                SR.INVALID_BLOB_TYPE, blob.properties.getBlobType(), retrievedAttributes.getProperties()
                                        .getBlobType()), Constants.HeaderConstants.HTTP_UNUSED_306, null, null);
                    }

                    blob.properties = retrievedAttributes.getProperties();
                    blob.metadata = retrievedAttributes.getMetadata();

                    return null;
                }
            };

            return getRequest;
        }

        @DoesServiceRequest
        private final StorageRequest<CloudBlobClient, CloudBlob, Integer> downloadToStreamImpl(final Long blobOffset,
                final Long length, final OutputStream outStream, final AccessCondition accessCondition,
                final BlobRequestOptions options, OperationContext opContext) {

            final long startingOffset = blobOffset == null ? 0 : blobOffset;
            final boolean isRangeGet = blobOffset != null;
            final StorageRequest<CloudBlobClient, CloudBlob, Integer> getRequest = new StorageRequest<CloudBlobClient, CloudBlob, Integer>(
                    options, this.getStorageUri()) {

                @Override
                public void setRequestLocationMode() {
                    this.setRequestLocationMode(RequestLocationMode.PRIMARY_OR_SECONDARY);
                }

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {

                    // The first time this is called, we have to set the length and blob offset. On retries, these will already have values and need not be called.
                    if (this.getOffset() == null) {
                        this.setOffset(blobOffset);
                    }

                    if (this.getLength() == null) {
                        this.setLength(length);
                    }

                    AccessCondition tempCondition = (this.getETagLockCondition() != null) ? this.getETagLockCondition()
                            : accessCondition;

                    return BlobRequest.getBlob(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                            options, context, tempCondition, blob.snapshotID, this.getOffset(), this.getLength(),
                            (options.getUseTransactionalContentMD5() && !this.getArePropertiesPopulated()));
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, -1L, null);
                }

                @Override
                public Integer preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    return preProcessDownloadResponse(this, options, client, blob, context, isRangeGet);
                }

                @Override
                public Integer postProcessResponse(HttpURLConnection connection, CloudBlob blob, CloudBlobClient client,
                        OperationContext context, Integer storageObject) throws Exception {
                    final Boolean validateMD5 = !options.getDisableContentMD5Validation()
                            && !Utility.isNullOrEmpty(this.getContentMD5());
                    final String contentLength = connection.getHeaderField(Constants.HeaderConstants.CONTENT_LENGTH);
                    final long expectedLength = Long.parseLong(contentLength);

                    Logger.info(context, String.format(SR.CREATING_NETWORK_STREAM, expectedLength));
                    final NetworkInputStream streamRef = new NetworkInputStream(connection.getInputStream(), expectedLength);

                    try {
                        // writeToOutputStream will update the currentRequestByteCount on this request in case a retry
                        // is needed and download should resume from that point
                        final StreamMd5AndLength descriptor = Utility.writeToOutputStream(streamRef, outStream, -1, false,
                                validateMD5, context, options, this);

                        // length was already checked by the NetworkInputStream, now check Md5
                        if (validateMD5 && !this.getContentMD5().equals(descriptor.getMd5())) {
                            throw new StorageException(StorageErrorCodeStrings.INVALID_MD5, String.format(
                                    SR.BLOB_HASH_MISMATCH, this.getContentMD5(), descriptor.getMd5()),
                                    Constants.HeaderConstants.HTTP_UNUSED_306, null, null);
                        }
                    }
                    finally {
                        // Close the stream and return. Closing an already closed stream is harmless. So its fine to try
                        // to drain the response and close the stream again in the executor.
                        streamRef.close();
                    }

                    return null;
                }

                @Override
                public void recoveryAction(OperationContext context) throws IOException {
                    if (this.getETagLockCondition() == null && (!Utility.isNullOrEmpty(this.getLockedETag()))) {
                        AccessCondition etagLockCondition = new AccessCondition();
                        etagLockCondition.setIfMatch(this.getLockedETag());
                        if (accessCondition != null) {
                            etagLockCondition.setLeaseID(accessCondition.getLeaseID());
                        }
                        this.setETagLockCondition(etagLockCondition);
                    }

                    if (this.getCurrentRequestByteCount() > 0) {
                        this.setOffset(startingOffset + this.getCurrentRequestByteCount());
                        if (length != null) {
                            this.setLength(length - this.getCurrentRequestByteCount());
                        }
                    }
                }
            };

            return getRequest;

        }


        @DoesServiceRequest
        protected final int downloadRangeInternal(final long blobOffset, final Long length, final byte[] buffer,
                final int bufferOffset, final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {

            if (bufferOffset < 0 || blobOffset < 0 || (length != null && length <= 0)) {
                throw new IndexOutOfBoundsException();
            }

            if (opContext == null) {
                opContext = new OperationContext();
            }

            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            if (options.getUseTransactionalContentMD5() && (length != null && length > 4 * Constants.MB)) {
                throw new IllegalArgumentException(SR.INVALID_RANGE_CONTENT_MD5_HEADER);
            }

            return ExecutionEngine.executeWithRetry(this.blobServiceClient, this, this.downloadToByteArrayImpl(blobOffset,
                            length, buffer, bufferOffset, accessCondition, options, opContext), options.getRetryPolicyFactory(),
                    opContext);
        }


        @DoesServiceRequest
        public final int downloadRangeToByteArray(final long offset, final Long length, final byte[] buffer,
                final int bufferOffet) throws StorageException {
            return this.downloadRangeToByteArray(offset, length, buffer, bufferOffet, null ,
                    null , null );
        }


        @DoesServiceRequest
        public final int downloadRangeToByteArray(final long offset, final Long length, final byte[] buffer,
                final int bufferOffset, final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {

            Utility.assertNotNull("buffer", buffer);

            if (length != null) {
                if (length + bufferOffset > buffer.length) {
                    throw new IndexOutOfBoundsException();
                }
            }

            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();

            return this.downloadRangeInternal(offset, length, buffer, bufferOffset, accessCondition, options, opContext);
        }


        @DoesServiceRequest
        public final int downloadToByteArray(final byte[] buffer, final int bufferOffet) throws StorageException {
            return this
                    .downloadToByteArray(buffer, bufferOffet, null , null , null );
        }


        @DoesServiceRequest
        public final int downloadToByteArray(final byte[] buffer, final int bufferOffset,
                final AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
                throws StorageException {

            Utility.assertNotNull("buffer", buffer);
            if (bufferOffset < 0) {
                throw new IndexOutOfBoundsException();
            }

            if (bufferOffset >= buffer.length) {
                throw new IndexOutOfBoundsException();
            }

            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.downloadToByteArrayImpl(null, null, buffer, bufferOffset, accessCondition, options, opContext),
                    options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, Integer> downloadToByteArrayImpl(final Long blobOffset,
                final Long length, final byte[] buffer, final int bufferOffset, final AccessCondition accessCondition,
                final BlobRequestOptions options, OperationContext opContext) {
            final long startingOffset = blobOffset == null ? 0 : blobOffset;
            final boolean isRangeGet = blobOffset != null;
            final StorageRequest<CloudBlobClient, CloudBlob, Integer> getRequest = new StorageRequest<CloudBlobClient, CloudBlob, Integer>(
                    options, this.getStorageUri()) {

                @Override
                public void setRequestLocationMode() {
                    this.setRequestLocationMode(RequestLocationMode.PRIMARY_OR_SECONDARY);
                }

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    // The first time this is called, we have to set the length and blob offset. On retries, these will already have values and need not be called.
                    if (this.getOffset() == null) {
                        this.setOffset(blobOffset);
                    }

                    if (this.getLength() == null) {
                        this.setLength(length);
                    }

                    AccessCondition tempCondition = (this.getETagLockCondition() != null) ? this.getETagLockCondition()
                            : accessCondition;
                    return BlobRequest.getBlob(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                            options, context, tempCondition, blob.snapshotID, this.getOffset(), this.getLength(),
                            (options.getUseTransactionalContentMD5() && !this.getArePropertiesPopulated()));
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, -1L, null);
                }

                @Override
                public Integer preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    return preProcessDownloadResponse(this, options, client, blob, context, isRangeGet);
                }

                @Override
                public Integer postProcessResponse(HttpURLConnection connection, CloudBlob blob, CloudBlobClient client,
                        OperationContext context, Integer storageObject) throws Exception {

                    final String contentLength = connection.getHeaderField(Constants.HeaderConstants.CONTENT_LENGTH);
                    final long expectedLength = Long.parseLong(contentLength);

                    Logger.info(context, String.format(SR.CREATING_NETWORK_STREAM, expectedLength));

                    final NetworkInputStream sourceStream = new NetworkInputStream(connection.getInputStream(),
                            expectedLength);

                    try {
                        int totalRead = 0;
                        int nextRead = buffer.length - bufferOffset;
                        int count = sourceStream.read(buffer, bufferOffset, nextRead);

                        while (count > 0) {
                            // if maximum execution time would be exceeded
                            if (Utility.validateMaxExecutionTimeout(options.getOperationExpiryTimeInMs())) {
                                // throw an exception
                                TimeoutException timeoutException = new TimeoutException(
                                        SR.MAXIMUM_EXECUTION_TIMEOUT_EXCEPTION);
                                throw Utility.initIOException(timeoutException);
                            }

                            totalRead += count;
                            this.setCurrentRequestByteCount(this.getCurrentRequestByteCount() + count);

                            nextRead = buffer.length - (bufferOffset + totalRead);

                            if (nextRead == 0) {
                                // check for case where more data is returned
                                if (sourceStream.read(new byte[1], 0, 1) != -1) {
                                    throw new StorageException(StorageErrorCodeStrings.OUT_OF_RANGE_INPUT,
                                            SR.CONTENT_LENGTH_MISMATCH, Constants.HeaderConstants.HTTP_UNUSED_306, null,
                                            null);
                                }
                            }

                            count = sourceStream.read(buffer, bufferOffset + totalRead, nextRead);
                        }

                        if (totalRead != expectedLength) {
                            throw new StorageException(StorageErrorCodeStrings.OUT_OF_RANGE_INPUT,
                                    SR.CONTENT_LENGTH_MISMATCH, Constants.HeaderConstants.HTTP_UNUSED_306, null, null);
                        }
                    }
                    finally {
                        // Close the stream. Closing an already closed stream is harmless. So its fine to try
                        // to drain the response and close the stream again in the executor.
                        sourceStream.close();
                    }

                    final Boolean validateMD5 = !options.getDisableContentMD5Validation()
                            && !Utility.isNullOrEmpty(this.getContentMD5());
                    if (validateMD5) {
                        try {
                            final MessageDigest digest = MessageDigest.getInstance("MD5");
                            digest.update(buffer, bufferOffset, (int) this.getCurrentRequestByteCount());

                            final String calculatedMD5 = Base64.encode(digest.digest());
                            if (!this.getContentMD5().equals(calculatedMD5)) {
                                throw new StorageException(StorageErrorCodeStrings.INVALID_MD5, String.format(
                                        SR.BLOB_HASH_MISMATCH, this.getContentMD5(), calculatedMD5),
                                        Constants.HeaderConstants.HTTP_UNUSED_306, null, null);
                            }
                        }
                        catch (final NoSuchAlgorithmException e) {
                            // This wont happen, throw fatal.
                            throw Utility.generateNewUnexpectedStorageException(e);
                        }
                    }

                    return (int) this.getCurrentRequestByteCount();
                }

                @Override
                public void recoveryAction(OperationContext context) throws IOException {
                    if (this.getETagLockCondition() == null && (!Utility.isNullOrEmpty(this.getLockedETag()))) {
                        AccessCondition etagLockCondition = new AccessCondition();
                        etagLockCondition.setIfMatch(this.getLockedETag());
                        if (accessCondition != null) {
                            etagLockCondition.setLeaseID(accessCondition.getLeaseID());
                        }
                        this.setETagLockCondition(etagLockCondition);
                    }

                    if (this.getCurrentRequestByteCount() > 0) {
                        this.setOffset(startingOffset + this.getCurrentRequestByteCount());
                        if (length != null) {
                            this.setLength(length - this.getCurrentRequestByteCount());
                        }
                    }
                }
            };
            return getRequest;
        }


        public void uploadFromByteArray(final byte[] buffer, final int offset, final int length) throws StorageException,
                IOException {
            uploadFromByteArray(buffer, offset, length, null , null , null );
        }


        public void uploadFromByteArray(final byte[] buffer, final int offset, final int length,
                final AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
                throws StorageException, IOException {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(buffer, offset, length);
            this.upload(inputStream, length, accessCondition, options, opContext);
            inputStream.close();
        }


        public void uploadFromFile(final String path) throws StorageException, IOException {
            uploadFromFile(path, null , null , null );
        }


        public void uploadFromFile(final String path, final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException, IOException {
            File file = new File(path);
            long fileLength = file.length();
            InputStream inputStream = new BufferedInputStream(new FileInputStream(file));
            this.upload(inputStream, fileLength, accessCondition, options, opContext);
            inputStream.close();
        }


        public void downloadToFile(final String path) throws StorageException, IOException {
            downloadToFile(path, null , null , null );
        }


        public void downloadToFile(final String path, final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException, IOException {
            OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(path));
            try {
                this.download(outputStream, accessCondition, options, opContext);
                outputStream.close();
            }
            catch (StorageException e) {
                deleteEmptyFileOnException(outputStream, path);
                throw e;
            }
            catch (IOException e) {
                deleteEmptyFileOnException(outputStream, path);
                throw e;
            }
        }


        private void deleteEmptyFileOnException(OutputStream outputStream, String path) {
            try {
                outputStream.close();
                File fileToDelete = new File(path);
                fileToDelete.delete();
            }
            catch (Exception e) {
                // Best effort delete.
            }
        }


        @DoesServiceRequest
        public final boolean exists() throws StorageException {
            return this.exists(null , null , null );
        }


        @DoesServiceRequest
        public final boolean exists(final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            return this.exists(false , accessCondition, options, opContext);
        }

        @DoesServiceRequest
        private final boolean exists(final boolean primaryOnly, final AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.existsImpl(primaryOnly, accessCondition, options), options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, Boolean> existsImpl(final boolean primaryOnly,
                final AccessCondition accessCondition, final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlob, Boolean> getRequest = new StorageRequest<CloudBlobClient, CloudBlob, Boolean>(
                    options, this.getStorageUri()) {
                @Override
                public void setRequestLocationMode() {
                    this.setRequestLocationMode(primaryOnly ? RequestLocationMode.PRIMARY_ONLY
                            : RequestLocationMode.PRIMARY_OR_SECONDARY);
                }

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    return BlobRequest.getBlobProperties(
                            blob.getTransformedAddress(context).getUri(this.getCurrentLocation()), options, context,
                            accessCondition, blob.snapshotID);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, -1L, null);
                }

                @Override
                public Boolean preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() == HttpURLConnection.HTTP_OK) {
                        final BlobAttributes retrievedAttributes = BlobResponse.getBlobAttributes(this.getConnection(),
                                blob.getStorageUri(), blob.snapshotID);
                        blob.properties = retrievedAttributes.getProperties();
                        blob.metadata = retrievedAttributes.getMetadata();
                        return Boolean.valueOf(true);
                    }
                    else if (this.getResult().getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                        return Boolean.valueOf(false);
                    }
                    else {
                        this.setNonExceptionedRetryableFailure(true);

                        // return false instead of null to avoid SCA issues
                        return false;
                    }
                }
            };

            return getRequest;
        }


        public String generateSharedAccessSignature(final SharedAccessBlobPolicy policy, final String groupPolicyIdentifier)
                throws InvalidKeyException, StorageException {

            return this.generateSharedAccessSignature(policy, null , groupPolicyIdentifier);
        }


        public String generateSharedAccessSignature(final SharedAccessBlobPolicy policy,
                final SharedAccessBlobHeaders headers, final String groupPolicyIdentifier) throws InvalidKeyException,
                StorageException {

            if (!StorageCredentialsHelper.canCredentialsSignRequest(this.blobServiceClient.getCredentials())) {
                throw new IllegalArgumentException(SR.CANNOT_CREATE_SAS_WITHOUT_ACCOUNT_KEY);
            }

            if (this.isSnapshot()) {
                throw new IllegalArgumentException(SR.CANNOT_CREATE_SAS_FOR_SNAPSHOTS);
            }

            final String resourceName = this.getCanonicalName(true);

            final String signature = SharedAccessSignatureHelper.generateSharedAccessSignatureHashForBlob(policy, headers,
                    groupPolicyIdentifier, resourceName, this.blobServiceClient, null);

            final UriQueryBuilder builder = SharedAccessSignatureHelper.generateSharedAccessSignatureForBlob(policy,
                    headers, groupPolicyIdentifier, "b", signature);

            return builder.toString();
        }


        String getCanonicalName(final boolean ignoreSnapshotTime) {
            String canonicalName;
            if (this.blobServiceClient.isUsePathStyleUris()) {
                canonicalName = this.getUri().getRawPath();
            }
            else {
                canonicalName = PathUtility.getCanonicalPathFromCredentials(this.blobServiceClient.getCredentials(), this
                        .getUri().getRawPath());
            }

            if (!ignoreSnapshotTime && this.snapshotID != null) {
                canonicalName = canonicalName.concat("?snapshot=");
                canonicalName = canonicalName.concat(this.snapshotID);
            }

            return canonicalName;
        }


        @Override
        public final CloudBlobContainer getContainer() throws StorageException, URISyntaxException {
            if (this.container == null) {
                final StorageUri containerURI = PathUtility.getContainerURI(this.getStorageUri(),
                        this.blobServiceClient.isUsePathStyleUris());
                this.container = new CloudBlobContainer(containerURI, this.blobServiceClient);
            }

            return this.container;
        }


        public final HashMap<String, String> getMetadata() {
            return this.metadata;
        }


        public final String getName() throws URISyntaxException {
            if (Utility.isNullOrEmpty(this.name)) {
                this.name = PathUtility.getBlobNameFromURI(this.getUri(), this.blobServiceClient.isUsePathStyleUris());
            }
            return this.name;
        }


        @Override
        public final CloudBlobDirectory getParent() throws URISyntaxException, StorageException {
            if (this.parent == null) {
                final String parentName = getParentNameFromURI(this.getStorageUri(),
                        this.blobServiceClient.getDirectoryDelimiter(), this.getContainer());

                if (parentName != null) {
                    StorageUri parentURI = PathUtility.appendPathToUri(this.container.getStorageUri(), parentName);
                    this.parent = new CloudBlobDirectory(parentURI, parentName, this.blobServiceClient, this.getContainer());
                }
            }
            return this.parent;
        }


        public final BlobProperties getProperties() {
            return this.properties;
        }


        public CopyState getCopyState() {
            return this.properties.getCopyState();
        }


        public final StorageUri getQualifiedStorageUri() throws URISyntaxException, StorageException {
            if (this.isSnapshot()) {
                StorageUri snapshotQualifiedUri = PathUtility.addToQuery(this.getStorageUri(),
                        String.format("snapshot=%s", this.snapshotID));
                return this.blobServiceClient.getCredentials().transformUri(snapshotQualifiedUri);
            }
            return this.blobServiceClient.getCredentials().transformUri(this.getStorageUri());
        }


        public final URI getQualifiedUri() throws URISyntaxException, StorageException {
            if (this.isSnapshot()) {
                return PathUtility.addToQuery(this.getUri(), String.format("snapshot=%s", this.snapshotID));
            }
            return this.blobServiceClient.getCredentials().transformUri(this.getUri());
        }


        public final CloudBlobClient getServiceClient() {
            return this.blobServiceClient;
        }


        public final String getSnapshotID() {
            return this.snapshotID;
        }


        @Override
        public final StorageUri getStorageUri() {
            return this.storageUri;
        }


        public final int getStreamWriteSizeInBytes() {
            return this.streamWriteSizeInBytes;
        }


        public final int getStreamMinimumReadSizeInBytes() {
            return this.streamMinimumReadSizeInBytes;
        }


        protected final StorageUri getTransformedAddress(final OperationContext opContext) throws URISyntaxException,
                StorageException {
            return this.blobServiceClient.getCredentials().transformUri(this.getStorageUri(), opContext);
        }


        @Override
        public final URI getUri() {
            return this.storageUri.getPrimaryUri();
        }


        public final boolean isSnapshot() {
            return this.snapshotID != null;
        }


        @DoesServiceRequest
        public final BlobInputStream openInputStream() throws StorageException {
            return this.openInputStream(null , null , null );
        }


        @DoesServiceRequest
        public final BlobInputStream openInputStream(final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            assertNoWriteOperationForSnapshot();

            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient,
                    false );

            return new BlobInputStream(this, accessCondition, options, opContext);
        }


        protected void parseURIQueryStringAndVerify(final StorageUri completeUri, final CloudBlobClient existingClient,
                final boolean usePathStyleUris) throws StorageException {
            Utility.assertNotNull("resourceUri", completeUri);

            if (!completeUri.isAbsolute()) {
                final String errorMessage = String.format(SR.RELATIVE_ADDRESS_NOT_PERMITTED, completeUri.toString());
                throw new IllegalArgumentException(errorMessage);
            }

            this.storageUri = PathUtility.stripURIQueryAndFragment(completeUri);

            final HashMap<String, String[]> queryParameters = PathUtility.parseQueryString(completeUri.getQuery());

            final StorageCredentialsSharedAccessSignature sasCreds = SharedAccessSignatureHelper
                    .parseQuery(queryParameters);

            final String[] snapshotIDs = queryParameters.get(BlobConstants.SNAPSHOT);
            if (snapshotIDs != null && snapshotIDs.length > 0) {
                this.snapshotID = snapshotIDs[0];
            }

            if (sasCreds == null && existingClient != null) {
                return;
            }

            final Boolean sameCredentials = existingClient == null ? false : Utility.areCredentialsEqual(sasCreds,
                    existingClient.getCredentials());

            if (existingClient == null || !sameCredentials) {
                try {
                    this.blobServiceClient = new CloudBlobClient((PathUtility.getServiceClientBaseAddress(
                            this.getStorageUri(), usePathStyleUris)), sasCreds);
                }
                catch (final URISyntaxException e) {
                    throw Utility.generateNewUnexpectedStorageException(e);
                }
            }

            if (existingClient != null && !sameCredentials) {
                this.blobServiceClient.setDefaultRequestOptions(new BlobRequestOptions(existingClient
                        .getDefaultRequestOptions()));
                this.blobServiceClient.setDirectoryDelimiter(existingClient.getDirectoryDelimiter());
            }
        }


        @DoesServiceRequest
        public final void releaseLease(final AccessCondition accessCondition) throws StorageException {
            this.releaseLease(accessCondition, null , null );
        }


        @DoesServiceRequest
        public final void releaseLease(final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            Utility.assertNotNull("accessCondition", accessCondition);
            Utility.assertNotNullOrEmpty("leaseID", accessCondition.getLeaseID());

            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this, this.releaseLeaseImpl(accessCondition, options),
                    options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, Void> releaseLeaseImpl(final AccessCondition accessCondition,
                final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlob, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, Void>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    return BlobRequest.leaseBlob(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                            options, context, accessCondition, LeaseAction.RELEASE, null ,
                            null , null );
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);
                }

                @Override
                public Void preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    updateEtagAndLastModifiedFromResponse(this.getConnection());

                    blob.properties.setLeaseStatus(LeaseStatus.UNLOCKED);
                    return null;
                }
            };

            return putRequest;
        }


        @DoesServiceRequest
        public final void renewLease(final AccessCondition accessCondition) throws StorageException {
            this.renewLease(accessCondition, null , null );
        }


        @DoesServiceRequest
        public final void renewLease(final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            Utility.assertNotNull("accessCondition", accessCondition);
            Utility.assertNotNullOrEmpty("leaseID", accessCondition.getLeaseID());

            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this, this.renewLeaseImpl(accessCondition, options),
                    options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, Void> renewLeaseImpl(final AccessCondition accessCondition,
                final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlob, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, Void>(
                    options, this.getStorageUri()) {
                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    return BlobRequest.leaseBlob(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                            options, context, accessCondition, LeaseAction.RENEW, null ,
                            null , null );
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);
                }

                @Override
                public Void preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    updateEtagAndLastModifiedFromResponse(this.getConnection());

                    return null;
                }
            };

            return putRequest;
        }


        protected final void setContainer(final CloudBlobContainer container) {
            this.container = container;
        }


        public final void setMetadata(final HashMap<String, String> metadata) {
            this.metadata = metadata;
        }


        protected final void setProperties(final BlobProperties properties) {
            this.properties = properties;
        }


        protected final void setSnapshotID(final String snapshotID) {
            this.snapshotID = snapshotID;
        }


        protected void setStorageUri(final StorageUri storageUri) {
            this.storageUri = storageUri;
        }


        public abstract void setStreamWriteSizeInBytes(int streamWriteSizeInBytes);


        public void setStreamMinimumReadSizeInBytes(final int minimumReadSize) {
            if (minimumReadSize < 16 * Constants.KB) {
                throw new IllegalArgumentException("MinimumReadSize");
            }

            this.streamMinimumReadSizeInBytes = minimumReadSize;
        }

        protected void updateEtagAndLastModifiedFromResponse(HttpURLConnection request) {
            // ETag
            this.getProperties().setEtag(request.getHeaderField(Constants.HeaderConstants.ETAG));

            // Last Modified
            if (0 != request.getLastModified()) {
                final Calendar lastModifiedCalendar = Calendar.getInstance(Utility.LOCALE_US);
                lastModifiedCalendar.setTimeZone(Utility.UTC_ZONE);
                lastModifiedCalendar.setTime(new Date(request.getLastModified()));
                this.getProperties().setLastModified(lastModifiedCalendar.getTime());
            }
        }

        protected void updateLengthFromResponse(HttpURLConnection request) {
            final String xContentLengthHeader = request.getHeaderField(BlobConstants.CONTENT_LENGTH_HEADER);
            if (!Utility.isNullOrEmpty(xContentLengthHeader)) {
                this.getProperties().setLength(Long.parseLong(xContentLengthHeader));
            }
        }


        @DoesServiceRequest
        public abstract void upload(InputStream sourceStream, long length) throws StorageException, IOException;


        @DoesServiceRequest
        public abstract void upload(InputStream sourceStream, long length, final AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException, IOException;



        @DoesServiceRequest
        public final void uploadMetadata() throws StorageException {
            this.uploadMetadata(null , null , null );
        }


        @DoesServiceRequest
        public final void uploadMetadata(final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            assertNoWriteOperationForSnapshot();

            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.uploadMetadataImpl(accessCondition, options), options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, Void> uploadMetadataImpl(final AccessCondition accessCondition,
                final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlob, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, Void>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    return BlobRequest.setBlobMetadata(blob.getTransformedAddress(context)
                            .getUri(this.getCurrentLocation()), options, context, accessCondition);
                }

                @Override
                public void setHeaders(HttpURLConnection connection, CloudBlob blob, OperationContext context) {
                    BlobRequest.addMetadata(connection, blob.metadata, context);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);
                }

                @Override
                public Void preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    blob.updateEtagAndLastModifiedFromResponse(this.getConnection());
                    return null;
                }
            };

            return putRequest;
        }


        @DoesServiceRequest
        public final void uploadProperties() throws StorageException {
            this.uploadProperties(null , null , null );
        }


        @DoesServiceRequest
        public final void uploadProperties(final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            assertNoWriteOperationForSnapshot();

            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.uploadPropertiesImpl(accessCondition, options), options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, Void> uploadPropertiesImpl(
                final AccessCondition accessCondition, final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlob, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, Void>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    return BlobRequest.setBlobProperties(
                            blob.getTransformedAddress(context).getUri(this.getCurrentLocation()), options, context,
                            accessCondition, blob.properties);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);

                }

                @Override
                public Void preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    blob.updateEtagAndLastModifiedFromResponse(this.getConnection());
                    return null;
                }
            };

            return putRequest;
        }

        private Integer preProcessDownloadResponse(final StorageRequest<CloudBlobClient, CloudBlob, Integer> request,
                final BlobRequestOptions options, final CloudBlobClient client, final CloudBlob blob,
                final OperationContext context, final boolean isRangeGet) throws StorageException, URISyntaxException,
                ParseException {
            if (request.getResult().getStatusCode() != HttpURLConnection.HTTP_PARTIAL
                    && request.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                request.setNonExceptionedRetryableFailure(true);
                return null;
            }

            if (!request.getArePropertiesPopulated()) {
                String originalContentMD5 = null;

                final BlobAttributes retrievedAttributes = BlobResponse.getBlobAttributes(request.getConnection(),
                        blob.getStorageUri(), blob.snapshotID);

                // Do not update Content-MD5 if it is a range get.
                if (isRangeGet) {
                    originalContentMD5 = blob.properties.getContentMD5();
                }
                else {
                    originalContentMD5 = retrievedAttributes.getProperties().getContentMD5();
                }

                if (!options.getDisableContentMD5Validation() && options.getUseTransactionalContentMD5()
                        && Utility.isNullOrEmpty(retrievedAttributes.getProperties().getContentMD5())) {
                    throw new StorageException(StorageErrorCodeStrings.MISSING_MD5_HEADER, SR.MISSING_MD5,
                            Constants.HeaderConstants.HTTP_UNUSED_306, null, null);
                }

                blob.properties = retrievedAttributes.getProperties();
                blob.metadata = retrievedAttributes.getMetadata();
                request.setContentMD5(retrievedAttributes.getProperties().getContentMD5());
                blob.properties.setContentMD5(originalContentMD5);
                request.setLockedETag(blob.properties.getEtag());
                request.setArePropertiesPopulated(true);
            }

            // If the download fails and Get Blob needs to resume the download, going to the
            // same storage location is important to prevent a possible ETag mismatch.
            request.setRequestLocationMode(request.getResult().getTargetLocation() == StorageLocation.PRIMARY ? RequestLocationMode.PRIMARY_ONLY
                    : RequestLocationMode.SECONDARY_ONLY);
            return null;
        }


        protected static String getParentNameFromURI(final StorageUri resourceAddress, final String delimiter,
                final CloudBlobContainer container) throws URISyntaxException {
            Utility.assertNotNull("resourceAddress", resourceAddress);
            Utility.assertNotNull("container", container);
            Utility.assertNotNullOrEmpty("delimiter", delimiter);

            String containerName = container.getName() + "/";

            String relativeURIString = Utility.safeRelativize(container.getStorageUri().getPrimaryUri(),
                    resourceAddress.getPrimaryUri());

            if (relativeURIString.endsWith(delimiter)) {
                relativeURIString = relativeURIString.substring(0, relativeURIString.length() - delimiter.length());
            }

            String parentName;

            if (Utility.isNullOrEmpty(relativeURIString)) {
                // Case 1 /<ContainerName>[Delimiter]*? => /<ContainerName>
                // Parent of container is container itself
                parentName = null;
            }
            else {
                final int lastDelimiterDex = relativeURIString.lastIndexOf(delimiter);

                if (lastDelimiterDex < 0) {
                    // Case 2 /<Container>/<folder>
                    // Parent of a folder is container
                    parentName = "";
                }
                else {
                    // Case 3 /<Container>/<folder>/[<subfolder>/]*<BlobName>
                    // Parent of blob is folder
                    parentName = relativeURIString.substring(0, lastDelimiterDex + delimiter.length());
                    if (parentName != null && parentName.equals(containerName)) {
                        parentName = "";
                    }
                }
            }

            return parentName;
        }
    }

    static class BlockListHandler extends DefaultHandler {

        private final Stack<String> elementStack = new Stack<String>();
        private StringBuilder bld = new StringBuilder();

        private final ArrayList<BlockEntry> blocks = new ArrayList<BlockEntry>();

        private BlockSearchMode searchMode;
        private String blockName;
        private Long blockSize;


        public static ArrayList<BlockEntry> getBlockList(InputStream streamRef) throws ParserConfigurationException,
                SAXException, IOException {
            SAXParser saxParser = Utility.getSAXParser();
            BlockListHandler handler = new BlockListHandler();
            saxParser.parse(streamRef, handler);

            return handler.blocks;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            this.elementStack.push(localName);

            if (BlobConstants.UNCOMMITTED_BLOCKS_ELEMENT.equals(localName)) {
                this.searchMode = BlockSearchMode.UNCOMMITTED;
            }
            else if (BlobConstants.COMMITTED_BLOCKS_ELEMENT.equals(localName)) {
                this.searchMode = BlockSearchMode.COMMITTED;
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            String currentNode = this.elementStack.pop();

            // if the node popped from the stack and the localName don't match, the xml document is improperly formatted
            if (!localName.equals(currentNode)) {
                throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
            }

            String value = this.bld.toString();
            if (value.isEmpty()) {
                value = null;
            }

            if (BlobConstants.BLOCK_ELEMENT.equals(currentNode)) {
                final BlockEntry newBlock = new BlockEntry(this.blockName, this.searchMode);
                newBlock.setSize(this.blockSize);
                this.blocks.add(newBlock);
            }
            else if (Constants.NAME_ELEMENT.equals(currentNode)) {
                this.blockName = value;
            }
            else if (BlobConstants.SIZE_ELEMENT.equals(currentNode)) {
                this.blockSize = Long.parseLong(value);
            }

            this.bld = new StringBuilder();
        }

        @Override
        public void characters(char ch[], int start, int length) throws SAXException {
            this.bld.append(ch, start, length);
        }
    }

    public static final class CloudBlobContainer {


        static BlobContainerPermissions getContainerAcl(final String aclString) {
            BlobContainerPublicAccessType accessType = BlobContainerPublicAccessType.OFF;

            if (!Utility.isNullOrEmpty(aclString)) {
                final String lowerAclString = aclString.toLowerCase();
                if ("container".equals(lowerAclString)) {
                    accessType = BlobContainerPublicAccessType.CONTAINER;
                }
                else if ("blob".equals(lowerAclString)) {
                    accessType = BlobContainerPublicAccessType.BLOB;
                }
                else {
                    throw new IllegalArgumentException(String.format(SR.INVALID_ACL_ACCESS_TYPE, aclString));
                }
            }

            final BlobContainerPermissions retVal = new BlobContainerPermissions();
            retVal.setPublicAccess(accessType);

            return retVal;
        }


        protected HashMap<String, String> metadata;


        BlobContainerProperties properties;


        String name;


        private StorageUri storageUri;


        private CloudBlobClient blobServiceClient;


        private CloudBlobContainer(final CloudBlobClient client) {
            this.metadata = new HashMap<String, String>();
            this.properties = new BlobContainerProperties();
            this.blobServiceClient = client;
        }


        public CloudBlobContainer(final URI uri) throws URISyntaxException, StorageException {
            this(new StorageUri(uri));
        }


        public CloudBlobContainer(final StorageUri storageUri) throws URISyntaxException, StorageException {
            this(storageUri, (CloudBlobClient) null );
        }


        public CloudBlobContainer(final String containerName, final CloudBlobClient client) throws URISyntaxException,
                StorageException {
            this(client);
            Utility.assertNotNull("client", client);
            Utility.assertNotNull("containerName", containerName);

            this.storageUri = PathUtility.appendPathToUri(client.getStorageUri(), containerName);

            this.name = containerName;
            this.parseQueryAndVerify(this.storageUri, client, client.isUsePathStyleUris());
        }


        public CloudBlobContainer(final URI uri, final CloudBlobClient client) throws URISyntaxException, StorageException {
            this(new StorageUri(uri), client);
        }


        public CloudBlobContainer(final StorageUri storageUri, final CloudBlobClient client) throws URISyntaxException,
                StorageException {
            this(client);

            Utility.assertNotNull("storageUri", storageUri);

            this.storageUri = storageUri;

            boolean usePathStyleUris = client == null ? Utility.determinePathStyleFromUri(this.storageUri.getPrimaryUri())
                    : client.isUsePathStyleUris();

            this.name = PathUtility.getContainerNameFromUri(storageUri.getPrimaryUri(), usePathStyleUris);

            this.parseQueryAndVerify(this.storageUri, client, usePathStyleUris);
        }


        @DoesServiceRequest
        public void create() throws StorageException {
            this.create(null , null );
        }


        @DoesServiceRequest
        public void create(BlobRequestOptions options, OperationContext opContext) throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this, createImpl(options),
                    options.getRetryPolicyFactory(), opContext);

        }

        private StorageRequest<CloudBlobClient, CloudBlobContainer, Void> createImpl(final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlobContainer, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlobContainer, Void>(
                    options, this.getStorageUri()) {
                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlobContainer container,
                        OperationContext context) throws Exception {
                    final HttpURLConnection request = BlobRequest.createContainer(
                            container.getTransformedAddress().getUri(this.getCurrentLocation()), options, context);
                    return request;
                }

                @Override
                public void setHeaders(HttpURLConnection connection, CloudBlobContainer container, OperationContext context) {
                    BlobRequest.addMetadata(connection, container.metadata, context);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);
                }

                @Override
                public Void preProcessResponse(CloudBlobContainer container, CloudBlobClient client,
                        OperationContext context) throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_CREATED) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    // Set attributes
                    final BlobContainerAttributes attributes = BlobResponse.getBlobContainerAttributes(
                            this.getConnection(), client.isUsePathStyleUris());
                    container.properties = attributes.getProperties();
                    container.name = attributes.getName();
                    return null;
                }
            };

            return putRequest;
        }


        @DoesServiceRequest
        public boolean createIfNotExists() throws StorageException {
            return this.createIfNotExists(null , null );
        }


        @DoesServiceRequest
        public boolean createIfNotExists(BlobRequestOptions options, OperationContext opContext) throws StorageException {
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this.blobServiceClient);

            boolean exists = this.exists(true , null , options, opContext);
            if (exists) {
                return false;
            }
            else {
                try {
                    this.create(options, opContext);
                    return true;
                }
                catch (StorageException e) {
                    if (e.getHttpStatusCode() == HttpURLConnection.HTTP_CONFLICT
                            && StorageErrorCodeStrings.CONTAINER_ALREADY_EXISTS.equals(e.getErrorCode())) {
                        return false;
                    }
                    else {
                        throw e;
                    }
                }
            }
        }


        @DoesServiceRequest
        public void delete() throws StorageException {
            this.delete(null , null , null );
        }


        @DoesServiceRequest
        public void delete(AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
                throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this, deleteImpl(accessCondition, options),
                    options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlobContainer, Void> deleteImpl(final AccessCondition accessCondition,
                final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlobContainer, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlobContainer, Void>(
                    options, this.getStorageUri()) {
                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlobContainer container,
                        OperationContext context) throws Exception {
                    return BlobRequest.deleteContainer(container.getStorageUri().getPrimaryUri(), options, context,
                            accessCondition);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, -1L, null);
                }

                @Override
                public Void preProcessResponse(CloudBlobContainer container, CloudBlobClient client,
                        OperationContext context) throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_ACCEPTED) {
                        this.setNonExceptionedRetryableFailure(true);
                    }

                    return null;
                }
            };

            return putRequest;
        }


        @DoesServiceRequest
        public boolean deleteIfExists() throws StorageException {
            return this.deleteIfExists(null , null , null );
        }


        @DoesServiceRequest
        public boolean deleteIfExists(AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this.blobServiceClient);

            boolean exists = this.exists(true , accessCondition, options, opContext);
            if (exists) {
                try {
                    this.delete(accessCondition, options, opContext);
                    return true;
                }
                catch (StorageException e) {
                    if (e.getHttpStatusCode() == HttpURLConnection.HTTP_NOT_FOUND
                            && StorageErrorCodeStrings.CONTAINER_NOT_FOUND.equals(e.getErrorCode())) {
                        return false;
                    }
                    else {
                        throw e;
                    }
                }
            }
            else {
                return false;
            }
        }


        @DoesServiceRequest
        public void downloadAttributes() throws StorageException {
            this.downloadAttributes(null , null , null );
        }


        @DoesServiceRequest
        public void downloadAttributes(AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.downloadAttributesImpl(accessCondition, options), options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlobContainer, Void> downloadAttributesImpl(
                final AccessCondition accessCondition, final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlobContainer, Void> getRequest = new StorageRequest<CloudBlobClient, CloudBlobContainer, Void>(
                    options, this.getStorageUri()) {

                @Override
                public void setRequestLocationMode() {
                    this.setRequestLocationMode(RequestLocationMode.PRIMARY_OR_SECONDARY);
                }

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlobContainer container,
                        OperationContext context) throws Exception {
                    return BlobRequest.getContainerProperties(
                            container.getTransformedAddress().getUri(this.getCurrentLocation()), options, context,
                            accessCondition);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, -1L, null);
                }

                @Override
                public Void preProcessResponse(CloudBlobContainer container, CloudBlobClient client,
                        OperationContext context) throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    // Set attributes
                    final BlobContainerAttributes attributes = BlobResponse.getBlobContainerAttributes(
                            this.getConnection(), client.isUsePathStyleUris());
                    container.metadata = attributes.getMetadata();
                    container.properties = attributes.getProperties();
                    container.name = attributes.getName();
                    return null;
                }
            };

            return getRequest;
        }


        @DoesServiceRequest
        public BlobContainerPermissions downloadPermissions() throws StorageException {
            return this.downloadPermissions(null , null , null );
        }


        @DoesServiceRequest
        public BlobContainerPermissions downloadPermissions(AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this.blobServiceClient);

            return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    downloadPermissionsImpl(accessCondition, options), options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlobContainer, BlobContainerPermissions> downloadPermissionsImpl(
                final AccessCondition accessCondition, final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlobContainer, BlobContainerPermissions> getRequest = new StorageRequest<CloudBlobClient, CloudBlobContainer, BlobContainerPermissions>(
                    options, this.getStorageUri()) {

                @Override
                public void setRequestLocationMode() {
                    this.setRequestLocationMode(RequestLocationMode.PRIMARY_OR_SECONDARY);
                }

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlobContainer container,
                        OperationContext context) throws Exception {
                    return BlobRequest.getAcl(container.getTransformedAddress().getUri(this.getCurrentLocation()), options,
                            accessCondition, context);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, -1L, null);
                }

                @Override
                public BlobContainerPermissions preProcessResponse(CloudBlobContainer container, CloudBlobClient client,
                        OperationContext context) throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                    }

                    container.updatePropertiesFromResponse(this.getConnection());
                    final String aclString = BlobResponse.getAcl(this.getConnection());
                    final BlobContainerPermissions containerAcl = getContainerAcl(aclString);
                    return containerAcl;
                }

                @Override
                public BlobContainerPermissions postProcessResponse(HttpURLConnection connection,
                        CloudBlobContainer container, CloudBlobClient client, OperationContext context,
                        BlobContainerPermissions containerAcl) throws Exception {
                    HashMap<String, SharedAccessBlobPolicy> accessIds = SharedAccessPolicyHandler.getAccessIdentifiers(this
                            .getConnection().getInputStream(), SharedAccessBlobPolicy.class);

                    for (final String key : accessIds.keySet()) {
                        containerAcl.getSharedAccessPolicies().put(key, accessIds.get(key));
                    }

                    return containerAcl;
                }
            };

            return getRequest;
        }


        @DoesServiceRequest
        public boolean exists() throws StorageException {
            return this.exists(null , null , null );
        }


        @DoesServiceRequest
        public boolean exists(final AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
                throws StorageException {
            return this.exists(false, accessCondition, options, opContext);
        }

        @DoesServiceRequest
        private boolean exists(final boolean primaryOnly, final AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this.blobServiceClient);

            return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.existsImpl(primaryOnly, accessCondition, options), options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlobContainer, Boolean> existsImpl(final boolean primaryOnly,
                final AccessCondition accessCondition, final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlobContainer, Boolean> getRequest = new StorageRequest<CloudBlobClient, CloudBlobContainer, Boolean>(
                    options, this.getStorageUri()) {

                @Override
                public void setRequestLocationMode() {
                    this.setRequestLocationMode(primaryOnly ? RequestLocationMode.PRIMARY_ONLY
                            : RequestLocationMode.PRIMARY_OR_SECONDARY);
                }

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlobContainer container,
                        OperationContext context) throws Exception {
                    return BlobRequest.getContainerProperties(
                            container.getTransformedAddress().getUri(this.getCurrentLocation()), options, context,
                            accessCondition);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, -1L, null);
                }

                @Override
                public Boolean preProcessResponse(CloudBlobContainer container, CloudBlobClient client,
                        OperationContext context) throws Exception {
                    if (this.getResult().getStatusCode() == HttpURLConnection.HTTP_OK) {
                        container.updatePropertiesFromResponse(this.getConnection());
                        return Boolean.valueOf(true);
                    }
                    else if (this.getResult().getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                        return Boolean.valueOf(false);
                    }
                    else {
                        this.setNonExceptionedRetryableFailure(true);
                        // return false instead of null to avoid SCA issues
                        return false;
                    }
                }
            };

            return getRequest;
        }


        public String generateSharedAccessSignature(final SharedAccessBlobPolicy policy, final String groupPolicyIdentifier)
                throws InvalidKeyException, StorageException {

            if (!StorageCredentialsHelper.canCredentialsSignRequest(this.blobServiceClient.getCredentials())) {
                final String errorMessage = SR.CANNOT_CREATE_SAS_WITHOUT_ACCOUNT_KEY;
                throw new IllegalArgumentException(errorMessage);
            }

            final String resourceName = this.getSharedAccessCanonicalName();

            final String signature = SharedAccessSignatureHelper.generateSharedAccessSignatureHashForBlob(policy,
                    null, groupPolicyIdentifier, resourceName, this.blobServiceClient, null);

            final UriQueryBuilder builder = SharedAccessSignatureHelper.generateSharedAccessSignatureForBlob(policy,
                    null, groupPolicyIdentifier, "c", signature);

            return builder.toString();
        }


        public CloudBlockBlob getBlockBlobReference(final String blobName) throws URISyntaxException, StorageException {
            Utility.assertNotNullOrEmpty("blobName", blobName);

            final StorageUri address = PathUtility.appendPathToUri(this.storageUri, blobName);

            return new CloudBlockBlob(address, this.blobServiceClient, this);
        }


        public CloudBlockBlob getBlockBlobReference(final String blobName, final String snapshotID)
                throws URISyntaxException, StorageException {
            Utility.assertNotNullOrEmpty("blobName", blobName);

            final StorageUri address = PathUtility.appendPathToUri(this.storageUri, blobName);

            final CloudBlockBlob retBlob = new CloudBlockBlob(address, snapshotID, this.blobServiceClient);
            retBlob.setContainer(this);
            return retBlob;
        }


        public CloudBlobDirectory getDirectoryReference(String directoryName) throws URISyntaxException {
            Utility.assertNotNull("directoryName", directoryName);

            // if the directory name does not end in the delimiter, add the delimiter
            if (!directoryName.isEmpty() && !directoryName.endsWith(this.blobServiceClient.getDirectoryDelimiter())) {
                directoryName = directoryName.concat(this.blobServiceClient.getDirectoryDelimiter());
            }

            final StorageUri address = PathUtility.appendPathToUri(this.storageUri, directoryName);

            return new CloudBlobDirectory(address, directoryName, this.blobServiceClient, this);
        }


        public HashMap<String, String> getMetadata() {
            return this.metadata;
        }


        public String getName() {
            return this.name;
        }


        public StorageUri getStorageUri() {
            return this.storageUri;
        }


        public CloudPageBlob getPageBlobReference(final String blobName) throws URISyntaxException, StorageException {
            Utility.assertNotNullOrEmpty("blobName", blobName);

            final StorageUri address = PathUtility.appendPathToUri(this.storageUri, blobName);

            return new CloudPageBlob(address, this.blobServiceClient, this);
        }


        public CloudPageBlob getPageBlobReference(final String blobName, final String snapshotID)
                throws URISyntaxException, StorageException {
            Utility.assertNotNullOrEmpty("blobName", blobName);

            final StorageUri address = PathUtility.appendPathToUri(this.storageUri, blobName);

            final CloudPageBlob retBlob = new CloudPageBlob(address, snapshotID, this.blobServiceClient);
            retBlob.setContainer(this);
            return retBlob;
        }


        public BlobContainerProperties getProperties() {
            return this.properties;
        }


        public CloudBlobClient getServiceClient() {
            return this.blobServiceClient;
        }


        private String getSharedAccessCanonicalName() {
            String accountName = this.getServiceClient().getCredentials().getAccountName();
            String containerName = this.getName();

            return String.format("/%s/%s", accountName, containerName);
        }


        private StorageUri getTransformedAddress() throws URISyntaxException, StorageException {
            return this.blobServiceClient.getCredentials().transformUri(this.storageUri);
        }


        public URI getUri() {
            return this.storageUri.getPrimaryUri();
        }


        @DoesServiceRequest
        public Iterable<ListBlobItem> listBlobs() {
            return this.listBlobs(null, false, EnumSet.noneOf(BlobListingDetails.class), null, null);
        }


        @DoesServiceRequest
        public Iterable<ListBlobItem> listBlobs(final String prefix) {
            return this.listBlobs(prefix, false, EnumSet.noneOf(BlobListingDetails.class), null, null);
        }


        @DoesServiceRequest
        public Iterable<ListBlobItem> listBlobs(final String prefix, final boolean useFlatBlobListing,
                final EnumSet<BlobListingDetails> listingDetails, BlobRequestOptions options, OperationContext opContext) {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this.blobServiceClient);

            if (!useFlatBlobListing && listingDetails != null && listingDetails.contains(BlobListingDetails.SNAPSHOTS)) {
                throw new IllegalArgumentException(SR.SNAPSHOT_LISTING_ERROR);
            }

            SegmentedStorageRequest segmentedRequest = new SegmentedStorageRequest();

            return new LazySegmentedIterable<CloudBlobClient, CloudBlobContainer, ListBlobItem>(
                    this.listBlobsSegmentedImpl(prefix, useFlatBlobListing, listingDetails, null, options, segmentedRequest),
                    this.blobServiceClient, this, options.getRetryPolicyFactory(), opContext);
        }


        @DoesServiceRequest
        public ResultSegment<ListBlobItem> listBlobsSegmented() throws StorageException {
            return this.listBlobsSegmented(null, false, EnumSet.noneOf(BlobListingDetails.class), null, null, null, null);
        }


        @DoesServiceRequest
        public ResultSegment<ListBlobItem> listBlobsSegmented(final String prefix) throws StorageException {
            return this.listBlobsSegmented(prefix, false, EnumSet.noneOf(BlobListingDetails.class), null, null, null, null);
        }


        @DoesServiceRequest
        public ResultSegment<ListBlobItem> listBlobsSegmented(final String prefix, final boolean useFlatBlobListing,
                final EnumSet<BlobListingDetails> listingDetails, final Integer maxResults,
                final ResultContinuation continuationToken, BlobRequestOptions options, OperationContext opContext)
                throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this.blobServiceClient);

            Utility.assertContinuationType(continuationToken, ResultContinuationType.BLOB);

            if (!useFlatBlobListing && listingDetails != null && listingDetails.contains(BlobListingDetails.SNAPSHOTS)) {
                throw new IllegalArgumentException(SR.SNAPSHOT_LISTING_ERROR);
            }

            SegmentedStorageRequest segmentedRequest = new SegmentedStorageRequest();
            segmentedRequest.setToken(continuationToken);

            return ExecutionEngine.executeWithRetry(this.blobServiceClient, this, this.listBlobsSegmentedImpl(prefix,
                    useFlatBlobListing, listingDetails, maxResults, options, segmentedRequest), options
                    .getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlobContainer, ResultSegment<ListBlobItem>> listBlobsSegmentedImpl(
                final String prefix, final boolean useFlatBlobListing, final EnumSet<BlobListingDetails> listingDetails,
                final Integer maxResults, final BlobRequestOptions options, final SegmentedStorageRequest segmentedRequest) {

            Utility.assertContinuationType(segmentedRequest.getToken(), ResultContinuationType.BLOB);
            Utility.assertNotNull("options", options);

            final String delimiter = useFlatBlobListing ? null : this.blobServiceClient.getDirectoryDelimiter();

            final BlobListingContext listingContext = new BlobListingContext(prefix, maxResults, delimiter, listingDetails);

            final StorageRequest<CloudBlobClient, CloudBlobContainer, ResultSegment<ListBlobItem>> getRequest = new StorageRequest<CloudBlobClient, CloudBlobContainer, ResultSegment<ListBlobItem>>(
                    options, this.getStorageUri()) {

                @Override
                public void setRequestLocationMode() {
                    this.setRequestLocationMode(Utility.getListingLocationMode(segmentedRequest.getToken()));
                }

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlobContainer container,
                        OperationContext context) throws Exception {
                    listingContext.setMarker(segmentedRequest.getToken() != null ? segmentedRequest.getToken()
                            .getNextMarker() : null);
                    return BlobRequest.listBlobs(container.getTransformedAddress().getUri(this.getCurrentLocation()),
                            options, context, listingContext);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, -1L, null);
                }

                @Override
                public ResultSegment<ListBlobItem> preProcessResponse(CloudBlobContainer container, CloudBlobClient client,
                        OperationContext context) throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                    }

                    return null;
                }

                @Override
                public ResultSegment<ListBlobItem> postProcessResponse(HttpURLConnection connection,
                        CloudBlobContainer container, CloudBlobClient client, OperationContext context,
                        ResultSegment<ListBlobItem> storageObject) throws Exception {
                    final ListBlobsResponse response = BlobListHandler.getBlobList(connection.getInputStream(), container);

                    ResultContinuation newToken = null;

                    if (response.getNextMarker() != null) {
                        newToken = new ResultContinuation();
                        newToken.setNextMarker(response.getNextMarker());
                        newToken.setContinuationType(ResultContinuationType.BLOB);
                        newToken.setTargetLocation(this.getResult().getTargetLocation());
                    }

                    final ResultSegment<ListBlobItem> resSegment = new ResultSegment<ListBlobItem>(response.getResults(),
                            response.getMaxResults(), newToken);

                    // Important for listBlobs because this is required by the lazy iterator between executions.
                    segmentedRequest.setToken(resSegment.getContinuationToken());

                    return resSegment;
                }
            };

            return getRequest;
        }


        @DoesServiceRequest
        public Iterable<CloudBlobContainer> listContainers() {
            return this.blobServiceClient.listContainers();
        }


        @DoesServiceRequest
        public Iterable<CloudBlobContainer> listContainers(final String prefix) {
            return this.blobServiceClient.listContainers(prefix);
        }


        @DoesServiceRequest
        public Iterable<CloudBlobContainer> listContainers(final String prefix,
                final ContainerListingDetails detailsIncluded, final BlobRequestOptions options,
                final OperationContext opContext) {
            return this.blobServiceClient.listContainers(prefix, detailsIncluded, options, opContext);
        }


        @DoesServiceRequest
        public ResultSegment<CloudBlobContainer> listContainersSegmented() throws StorageException {
            return this.blobServiceClient.listContainersSegmented();
        }


        @DoesServiceRequest
        public ResultSegment<CloudBlobContainer> listContainersSegmented(final String prefix) throws StorageException {
            return this.blobServiceClient.listContainersSegmented(prefix);
        }


        @DoesServiceRequest
        public ResultSegment<CloudBlobContainer> listContainersSegmented(final String prefix,
                final ContainerListingDetails detailsIncluded, final Integer maxResults,
                final ResultContinuation continuationToken, final BlobRequestOptions options,
                final OperationContext opContext) throws StorageException {
            return this.blobServiceClient.listContainersSegmented(prefix, detailsIncluded, maxResults, continuationToken,
                    options, opContext);
        }


        private void parseQueryAndVerify(final StorageUri completeUri, final CloudBlobClient existingClient,
                final boolean usePathStyleUris) throws URISyntaxException, StorageException {
            Utility.assertNotNull("completeUri", completeUri);

            if (!completeUri.isAbsolute()) {
                final String errorMessage = String.format(SR.RELATIVE_ADDRESS_NOT_PERMITTED, completeUri.toString());
                throw new IllegalArgumentException(errorMessage);
            }

            this.storageUri = PathUtility.stripURIQueryAndFragment(completeUri);

            final HashMap<String, String[]> queryParameters = PathUtility.parseQueryString(completeUri.getQuery());
            final StorageCredentialsSharedAccessSignature sasCreds = SharedAccessSignatureHelper
                    .parseQuery(queryParameters);

            if (sasCreds == null) {
                if (existingClient == null) {
                    this.blobServiceClient = new CloudBlobClient(new URI(PathUtility.getServiceClientBaseAddress(
                            this.getUri(), usePathStyleUris)));
                }
                return;
            }

            final Boolean sameCredentials = existingClient == null ? false : Utility.areCredentialsEqual(sasCreds,
                    existingClient.getCredentials());

            if (existingClient == null || !sameCredentials) {
                this.blobServiceClient = new CloudBlobClient(new URI(PathUtility.getServiceClientBaseAddress(this.getUri(),
                        usePathStyleUris)), sasCreds);
            }

            if (existingClient != null && !sameCredentials) {
                this.blobServiceClient.setDefaultRequestOptions(new BlobRequestOptions(existingClient
                        .getDefaultRequestOptions()));
                this.blobServiceClient.setDirectoryDelimiter(existingClient.getDirectoryDelimiter());
            }
        }

        void updatePropertiesFromResponse(HttpURLConnection request) {
            // ETag
            this.getProperties().setEtag(request.getHeaderField(Constants.HeaderConstants.ETAG));

            // Last Modified
            if (0 != request.getLastModified()) {
                final Calendar lastModifiedCalendar = Calendar.getInstance(Utility.LOCALE_US);
                lastModifiedCalendar.setTimeZone(Utility.UTC_ZONE);
                lastModifiedCalendar.setTime(new Date(request.getLastModified()));
                this.getProperties().setLastModified(lastModifiedCalendar.getTime());
            }
        }


        public void setMetadata(final HashMap<String, String> metadata) {
            this.metadata = metadata;
        }


        protected void setName(final String name) {
            this.name = name;
        }


        protected void setStorageUri(final StorageUri storageUri) {
            this.storageUri = storageUri;
        }


        protected void setProperties(final BlobContainerProperties properties) {
            this.properties = properties;
        }


        @DoesServiceRequest
        public void uploadMetadata() throws StorageException {
            this.uploadMetadata(null , null , null );
        }


        @DoesServiceRequest
        public void uploadMetadata(AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
                throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.uploadMetadataImpl(accessCondition, options), options.getRetryPolicyFactory(), opContext);

        }

        @DoesServiceRequest
        private StorageRequest<CloudBlobClient, CloudBlobContainer, Void> uploadMetadataImpl(
                final AccessCondition accessCondition, final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlobContainer, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlobContainer, Void>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlobContainer container,
                        OperationContext context) throws Exception {
                    return BlobRequest.setContainerMetadata(
                            container.getTransformedAddress().getUri(this.getCurrentLocation()), options, context,
                            accessCondition);
                }

                @Override
                public void setHeaders(HttpURLConnection connection, CloudBlobContainer container, OperationContext context) {
                    BlobRequest.addMetadata(connection, container.metadata, context);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);
                }

                @Override
                public Void preProcessResponse(CloudBlobContainer container, CloudBlobClient client,
                        OperationContext context) throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                    }

                    container.updatePropertiesFromResponse(this.getConnection());
                    return null;
                }
            };

            return putRequest;
        }


        @DoesServiceRequest
        public void uploadPermissions(final BlobContainerPermissions permissions) throws StorageException {
            this.uploadPermissions(permissions, null , null , null );
        }


        @DoesServiceRequest
        public void uploadPermissions(final BlobContainerPermissions permissions, final AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    uploadPermissionsImpl(permissions, accessCondition, options), options.getRetryPolicyFactory(),
                    opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlobContainer, Void> uploadPermissionsImpl(
                final BlobContainerPermissions permissions, final AccessCondition accessCondition,
                final BlobRequestOptions options) throws StorageException {
            try {
                final StringWriter outBuffer = new StringWriter();
                SharedAccessPolicySerializer.writeSharedAccessIdentifiersToStream(permissions.getSharedAccessPolicies(),
                        outBuffer);
                final byte[] aclBytes = outBuffer.toString().getBytes(Constants.UTF8_CHARSET);
                final StorageRequest<CloudBlobClient, CloudBlobContainer, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlobContainer, Void>(
                        options, this.getStorageUri()) {
                    @Override
                    public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlobContainer container,
                            OperationContext context) throws Exception {
                        this.setSendStream(new ByteArrayInputStream(aclBytes));
                        this.setLength((long) aclBytes.length);
                        return BlobRequest.setAcl(container.getTransformedAddress().getUri(this.getCurrentLocation()),
                                options, context, accessCondition, permissions.getPublicAccess());
                    }

                    @Override
                    public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                            throws Exception {
                        StorageRequest.signBlobQueueAndFileRequest(connection, client, aclBytes.length, null);
                    }

                    @Override
                    public Void preProcessResponse(CloudBlobContainer container, CloudBlobClient client,
                            OperationContext context) throws Exception {
                        if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                            this.setNonExceptionedRetryableFailure(true);
                            return null;
                        }

                        container.updatePropertiesFromResponse(this.getConnection());
                        return null;
                    }
                };

                return putRequest;
            }
            catch (final IllegalArgumentException e) {
                StorageException translatedException = StorageException.translateClientException(e);
                throw translatedException;
            }
            catch (final XMLStreamException e) {
                // The request was not even made. There was an error while trying to read the permissions. Just throw.
                StorageException translatedException = StorageException.translateClientException(e);
                throw translatedException;
            }
            catch (UnsupportedEncodingException e) {
                // The request was not even made. There was an error while trying to read the permissions. Just throw.
                StorageException translatedException = StorageException.translateClientException(e);
                throw translatedException;
            }
        }


        @DoesServiceRequest
        public final String acquireLease(final Integer leaseTimeInSeconds, final String proposedLeaseId)
                throws StorageException {
            return this.acquireLease(leaseTimeInSeconds, proposedLeaseId, null , null ,
                    null );
        }


        @DoesServiceRequest
        public final String acquireLease(final Integer leaseTimeInSeconds, final String proposedLeaseId,
                final AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
                throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this.blobServiceClient);

            return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.acquireLeaseImpl(leaseTimeInSeconds, proposedLeaseId, accessCondition, options),
                    options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlobContainer, String> acquireLeaseImpl(
                final Integer leaseTimeInSeconds, final String proposedLeaseId, final AccessCondition accessCondition,
                final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlobContainer, String> putRequest = new StorageRequest<CloudBlobClient, CloudBlobContainer, String>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlobContainer container,
                        OperationContext context) throws Exception {
                    return BlobRequest.leaseContainer(container.getTransformedAddress().getUri(this.getCurrentLocation()),
                            options, context, accessCondition, LeaseAction.ACQUIRE, leaseTimeInSeconds, proposedLeaseId,
                            null );
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);
                }

                @Override
                public String preProcessResponse(CloudBlobContainer container, CloudBlobClient client,
                        OperationContext context) throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_CREATED) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    container.updatePropertiesFromResponse(this.getConnection());

                    return BlobResponse.getLeaseID(this.getConnection());
                }

            };

            return putRequest;
        }


        @DoesServiceRequest
        public final void renewLease(final AccessCondition accessCondition) throws StorageException {
            this.renewLease(accessCondition, null , null );
        }


        @DoesServiceRequest
        public final void renewLease(final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            Utility.assertNotNull("accessCondition", accessCondition);
            Utility.assertNotNullOrEmpty("leaseID", accessCondition.getLeaseID());

            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this, this.renewLeaseImpl(accessCondition, options),
                    options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlobContainer, Void> renewLeaseImpl(
                final AccessCondition accessCondition, final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlobContainer, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlobContainer, Void>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlobContainer container,
                        OperationContext context) throws Exception {
                    return BlobRequest.leaseContainer(container.getTransformedAddress().getUri(this.getCurrentLocation()),
                            options, context, accessCondition, LeaseAction.RENEW, null ,
                            null , null );
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);
                }

                @Override
                public Void preProcessResponse(CloudBlobContainer container, CloudBlobClient client,
                        OperationContext context) throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    container.updatePropertiesFromResponse(this.getConnection());
                    return null;
                }
            };

            return putRequest;
        }


        @DoesServiceRequest
        public final void releaseLease(final AccessCondition accessCondition) throws StorageException {
            this.releaseLease(accessCondition, null , null );
        }


        @DoesServiceRequest
        public final void releaseLease(final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            Utility.assertNotNull("accessCondition", accessCondition);
            Utility.assertNotNullOrEmpty("leaseID", accessCondition.getLeaseID());

            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this, this.releaseLeaseImpl(accessCondition, options),
                    options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlobContainer, Void> releaseLeaseImpl(
                final AccessCondition accessCondition, final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlobContainer, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlobContainer, Void>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlobContainer container,
                        OperationContext context) throws Exception {
                    return BlobRequest.leaseContainer(container.getTransformedAddress().getUri(this.getCurrentLocation()),
                            options, context, accessCondition, LeaseAction.RELEASE, null, null, null);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);
                }

                @Override
                public Void preProcessResponse(CloudBlobContainer container, CloudBlobClient client,
                        OperationContext context) throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    container.updatePropertiesFromResponse(this.getConnection());
                    return null;
                }
            };

            return putRequest;
        }


        @DoesServiceRequest
        public final long breakLease(final Integer breakPeriodInSeconds) throws StorageException {
            return this.breakLease(breakPeriodInSeconds, null , null, null);
        }


        @DoesServiceRequest
        public final long breakLease(final Integer breakPeriodInSeconds, final AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            if (breakPeriodInSeconds != null) {
                Utility.assertGreaterThanOrEqual("breakPeriodInSeconds", breakPeriodInSeconds, 0);
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this.blobServiceClient);

            return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.breakLeaseImpl(breakPeriodInSeconds, accessCondition, options), options.getRetryPolicyFactory(),
                    opContext);
        }

        private final StorageRequest<CloudBlobClient, CloudBlobContainer, Long> breakLeaseImpl(
                final Integer breakPeriodInSeconds, final AccessCondition accessCondition, final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlobContainer, Long> putCmd = new StorageRequest<CloudBlobClient, CloudBlobContainer, Long>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlobContainer container,
                        OperationContext context) throws Exception {
                    return BlobRequest.leaseContainer(container.getTransformedAddress().getUri(this.getCurrentLocation()),
                            options, context, accessCondition, LeaseAction.BREAK, null, null, breakPeriodInSeconds);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);
                }

                @Override
                public Long preProcessResponse(CloudBlobContainer container, CloudBlobClient client,
                        OperationContext context) throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_ACCEPTED) {
                        this.setNonExceptionedRetryableFailure(true);
                        return -1L;
                    }

                    container.updatePropertiesFromResponse(this.getConnection());
                    final String leaseTime = BlobResponse.getLeaseTime(this.getConnection());
                    return Utility.isNullOrEmpty(leaseTime) ? -1L : Long.parseLong(leaseTime);
                }
            };

            return putCmd;
        }


        @DoesServiceRequest
        public final String changeLease(final String proposedLeaseId, final AccessCondition accessCondition)
                throws StorageException {
            return this.changeLease(proposedLeaseId, accessCondition, null, null);
        }


        @DoesServiceRequest
        public final String changeLease(final String proposedLeaseId, final AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException {
            Utility.assertNotNull("proposedLeaseId", proposedLeaseId);
            Utility.assertNotNull("accessCondition", accessCondition);
            Utility.assertNotNullOrEmpty("leaseID", accessCondition.getLeaseID());

            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this.blobServiceClient);

            return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.changeLeaseImpl(proposedLeaseId, accessCondition, options), options.getRetryPolicyFactory(),
                    opContext);
        }

        private final StorageRequest<CloudBlobClient, CloudBlobContainer, String> changeLeaseImpl(
                final String proposedLeaseId, final AccessCondition accessCondition, final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlobContainer, String> putRequest = new StorageRequest<CloudBlobClient, CloudBlobContainer, String>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlobContainer container,
                        OperationContext context) throws Exception {
                    return BlobRequest.leaseContainer(container.getTransformedAddress().getUri(this.getCurrentLocation()),
                            options, context, accessCondition, LeaseAction.CHANGE, null, proposedLeaseId, null);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);
                }

                @Override
                public String preProcessResponse(CloudBlobContainer container, CloudBlobClient client,
                        OperationContext context) throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    container.updatePropertiesFromResponse(this.getConnection());
                    return BlobResponse.getLeaseID(this.getConnection());
                }
            };

            return putRequest;
        }
    }

    public static final class SharedAccessBlobPolicy extends SharedAccessPolicy {

        private EnumSet<SharedAccessBlobPermissions> permissions;


        public EnumSet<SharedAccessBlobPermissions> getPermissions() {
            return this.permissions;
        }


        public void setPermissions(final EnumSet<SharedAccessBlobPermissions> permissions) {
            this.permissions = permissions;
        }


        @Override
        public String permissionsToString() {
            if (this.permissions == null) {
                return Constants.EMPTY_STRING;
            }

            // The service supports a fixed order => rwdl
            final StringBuilder builder = new StringBuilder();

            if (this.permissions.contains(SharedAccessBlobPermissions.READ)) {
                builder.append("r");
            }

            if (this.permissions.contains(SharedAccessBlobPermissions.WRITE)) {
                builder.append("w");
            }

            if (this.permissions.contains(SharedAccessBlobPermissions.DELETE)) {
                builder.append("d");
            }

            if (this.permissions.contains(SharedAccessBlobPermissions.LIST)) {
                builder.append("l");
            }

            return builder.toString();
        }


        @Override
        public void setPermissionsFromString(final String value) {
            final char[] chars = value.toCharArray();
            final EnumSet<SharedAccessBlobPermissions> retSet = EnumSet.noneOf(SharedAccessBlobPermissions.class);

            for (final char c : chars) {
                switch (c) {
                    case 'r':
                        retSet.add(SharedAccessBlobPermissions.READ);
                        break;
                    case 'w':
                        retSet.add(SharedAccessBlobPermissions.WRITE);
                        break;
                    case 'd':
                        retSet.add(SharedAccessBlobPermissions.DELETE);
                        break;
                    case 'l':
                        retSet.add(SharedAccessBlobPermissions.LIST);
                        break;
                    default:
                        throw new IllegalArgumentException("value");
                }
            }

            this.permissions = retSet;
        }
    }

    public static final class BlobRequestOptions extends RequestOptions {


        private Integer concurrentRequestCount = null;


        private Boolean useTransactionalContentMD5 = null;


        private Boolean storeBlobContentMD5 = null;


        private Boolean disableContentMD5Validation = null;


        private Integer singleBlobPutThresholdInBytes = null;


        public BlobRequestOptions() {
            // Empty Default Constructor.
        }


        public BlobRequestOptions(final BlobRequestOptions other) {
            super(other);
            if (other != null) {
                this.setConcurrentRequestCount(other.getConcurrentRequestCount());
                this.setUseTransactionalContentMD5(other.getUseTransactionalContentMD5());
                this.setStoreBlobContentMD5(other.getStoreBlobContentMD5());
                this.setDisableContentMD5Validation(other.getDisableContentMD5Validation());
                this.setSingleBlobPutThresholdInBytes(other.getSingleBlobPutThresholdInBytes());
            }
        }


        protected static final BlobRequestOptions applyDefaults(final BlobRequestOptions options, final BlobType blobType,
                final CloudBlobClient client) {
            return BlobRequestOptions.applyDefaults(options, blobType, client, true);
        }


        protected static final BlobRequestOptions applyDefaults(final BlobRequestOptions options, final BlobType blobType,
                final CloudBlobClient client, final boolean setStartTime) {
            BlobRequestOptions modifiedOptions = new BlobRequestOptions(options);
            BlobRequestOptions.populateRequestOptions(modifiedOptions, client.getDefaultRequestOptions(), setStartTime);
            return BlobRequestOptions.applyDefaultsInternal(modifiedOptions, blobType, client);
        }

        private static final BlobRequestOptions applyDefaultsInternal(final BlobRequestOptions modifiedOptions,
                final BlobType blobtype, final CloudBlobClient client) {
            Utility.assertNotNull("modifiedOptions", modifiedOptions);
            RequestOptions.applyBaseDefaultsInternal(modifiedOptions);
            if (modifiedOptions.getConcurrentRequestCount() == null) {
                modifiedOptions.setConcurrentRequestCount(BlobConstants.DEFAULT_CONCURRENT_REQUEST_COUNT);
            }

            if (modifiedOptions.getSingleBlobPutThresholdInBytes() == null) {
                modifiedOptions.setSingleBlobPutThresholdInBytes(BlobConstants.DEFAULT_SINGLE_BLOB_PUT_THRESHOLD_IN_BYTES);
            }

            if (modifiedOptions.getUseTransactionalContentMD5() == null) {
                modifiedOptions.setUseTransactionalContentMD5(false);
            }

            if (modifiedOptions.getStoreBlobContentMD5() == null) {
                modifiedOptions.setStoreBlobContentMD5(blobtype == BlobType.BLOCK_BLOB);
            }

            if (modifiedOptions.getDisableContentMD5Validation() == null) {
                modifiedOptions.setDisableContentMD5Validation(false);
            }

            return modifiedOptions;
        }


        private static final BlobRequestOptions populateRequestOptions(BlobRequestOptions modifiedOptions,
                final BlobRequestOptions clientOptions, final boolean setStartTime) {
            RequestOptions.populateRequestOptions(modifiedOptions, clientOptions, setStartTime);
            if (modifiedOptions.getConcurrentRequestCount() == null) {
                modifiedOptions.setConcurrentRequestCount(clientOptions.getConcurrentRequestCount());
            }

            if (modifiedOptions.getSingleBlobPutThresholdInBytes() == null) {
                modifiedOptions.setSingleBlobPutThresholdInBytes(clientOptions.getSingleBlobPutThresholdInBytes());
            }

            if (modifiedOptions.getUseTransactionalContentMD5() == null) {
                modifiedOptions.setUseTransactionalContentMD5(clientOptions.getUseTransactionalContentMD5());
            }

            if (modifiedOptions.getStoreBlobContentMD5() == null) {
                modifiedOptions.setStoreBlobContentMD5(clientOptions.getStoreBlobContentMD5());
            }

            if (modifiedOptions.getDisableContentMD5Validation() == null) {
                modifiedOptions.setDisableContentMD5Validation(clientOptions.getDisableContentMD5Validation());
            }

            return modifiedOptions;
        }


        public Integer getConcurrentRequestCount() {
            return this.concurrentRequestCount;
        }


        public Boolean getUseTransactionalContentMD5() {
            return this.useTransactionalContentMD5;
        }


        public Boolean getStoreBlobContentMD5() {
            return this.storeBlobContentMD5;
        }


        public Boolean getDisableContentMD5Validation() {
            return this.disableContentMD5Validation;
        }


        public Integer getSingleBlobPutThresholdInBytes() {
            return this.singleBlobPutThresholdInBytes;
        }


        public void setConcurrentRequestCount(final Integer concurrentRequestCount) {
            this.concurrentRequestCount = concurrentRequestCount;
        }


        public void setUseTransactionalContentMD5(final Boolean useTransactionalContentMD5) {
            this.useTransactionalContentMD5 = useTransactionalContentMD5;
        }


        public void setStoreBlobContentMD5(final Boolean storeBlobContentMD5) {
            this.storeBlobContentMD5 = storeBlobContentMD5;
        }


        public void setDisableContentMD5Validation(final Boolean disableContentMD5Validation) {
            this.disableContentMD5Validation = disableContentMD5Validation;
        }


        public void setSingleBlobPutThresholdInBytes(final Integer singleBlobPutThresholdInBytes) {
            if (singleBlobPutThresholdInBytes != null
                    && (singleBlobPutThresholdInBytes > BlobConstants.MAX_SINGLE_UPLOAD_BLOB_SIZE_IN_BYTES || singleBlobPutThresholdInBytes < 1 * Constants.MB)) {
                throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.ARGUMENT_OUT_OF_RANGE_ERROR,
                        "singleBlobPutThresholdInBytes", singleBlobPutThresholdInBytes.toString()));
            }

            this.singleBlobPutThresholdInBytes = singleBlobPutThresholdInBytes;
        }
    }

    public static enum ContainerListingDetails {

        ALL(1),


        METADATA(1),


        NONE(0);


        public int value;


        ContainerListingDetails(final int val) {
            this.value = val;
        }
    }

    public static enum CopyStatus {

        UNSPECIFIED,


        INVALID,


        PENDING,


        SUCCESS,


        ABORTED,


        FAILED;


        protected static CopyStatus parse(final String typeString) {
            if (Utility.isNullOrEmpty(typeString)) {
                return UNSPECIFIED;
            }
            else if ("invalid".equals(typeString.toLowerCase(Locale.US))) {
                return INVALID;
            }
            else if ("pending".equals(typeString.toLowerCase(Locale.US))) {
                return PENDING;
            }
            else if ("success".equals(typeString.toLowerCase(Locale.US))) {
                return SUCCESS;
            }
            else if ("aborted".equals(typeString.toLowerCase(Locale.US))) {
                return ABORTED;
            }
            else if ("failed".equals(typeString.toLowerCase(Locale.US))) {
                return FAILED;
            }
            else {
                return UNSPECIFIED;
            }
        }
    }

    static final class BlobRequest {

        private static final String BLOCK_QUERY_ELEMENT_NAME = "block";

        private static final String BLOCK_ID_QUERY_ELEMENT_NAME = "blockid";

        private static final String BLOCK_LIST_QUERY_ELEMENT_NAME = "blocklist";

        private static final String BLOCK_LIST_TYPE_QUERY_ELEMENT_NAME = "blocklisttype";

        private static final String COPY_QUERY_ELEMENT_NAME = "copy";

        private static final String PAGE_QUERY_ELEMENT_NAME = "page";

        private static final String PAGE_LIST_QUERY_ELEMENT_NAME = "pagelist";

        private static final String SNAPSHOTS_QUERY_ELEMENT_NAME = "snapshots";

        private static final String UNCOMMITTED_BLOBS_QUERY_ELEMENT_NAME = "uncommittedblobs";


        public static HttpURLConnection abortCopy(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition, final String copyId)
                throws StorageException, IOException, URISyntaxException {

            final UriQueryBuilder builder = new UriQueryBuilder();

            builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.COPY);
            builder.add(Constants.QueryConstants.COPY_ID, copyId);

            final HttpURLConnection request = BaseRequest.createURLConnection(uri, blobOptions, builder, opContext);

            request.setFixedLengthStreamingMode(0);
            request.setDoOutput(true);
            request.setRequestMethod(Constants.HTTP_PUT);

            request.setRequestProperty(Constants.HeaderConstants.COPY_ACTION_HEADER,
                    Constants.HeaderConstants.COPY_ACTION_ABORT);

            if (accessCondition != null) {
                accessCondition.applyLeaseConditionToRequest(request);
            }

            return request;
        }


        public static void addMetadata(final HttpURLConnection request, final HashMap<String, String> metadata,
                final OperationContext opContext) {
            BaseRequest.addMetadata(request, metadata, opContext);
        }


        private static void addProperties(final HttpURLConnection request, BlobProperties properties) {
            BaseRequest.addOptionalHeader(request, Constants.HeaderConstants.CACHE_CONTROL_HEADER,
                    properties.getCacheControl());
            BaseRequest.addOptionalHeader(request, BlobConstants.CONTENT_DISPOSITION_HEADER,
                    properties.getContentDisposition());
            BaseRequest.addOptionalHeader(request, BlobConstants.CONTENT_ENCODING_HEADER, properties.getContentEncoding());
            BaseRequest.addOptionalHeader(request, BlobConstants.CONTENT_LANGUAGE_HEADER, properties.getContentLanguage());
            BaseRequest.addOptionalHeader(request, BlobConstants.BLOB_CONTENT_MD5_HEADER, properties.getContentMD5());
            BaseRequest.addOptionalHeader(request, BlobConstants.CONTENT_TYPE_HEADER, properties.getContentType());
        }


        private static void addSnapshot(final UriQueryBuilder builder, final String snapshotVersion)
                throws StorageException {
            if (snapshotVersion != null) {
                builder.add(Constants.QueryConstants.SNAPSHOT, snapshotVersion);
            }
        }


        public static HttpURLConnection copyFrom(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition sourceAccessCondition,
                final AccessCondition destinationAccessCondition, String source, final String sourceSnapshotID)
                throws StorageException, IOException, URISyntaxException {

            if (sourceSnapshotID != null) {
                source = source.concat("?snapshot=");
                source = source.concat(sourceSnapshotID);
            }

            final HttpURLConnection request = BaseRequest.createURLConnection(uri, blobOptions, null, opContext);

            request.setFixedLengthStreamingMode(0);
            request.setDoOutput(true);
            request.setRequestMethod(Constants.HTTP_PUT);

            request.setRequestProperty(Constants.HeaderConstants.COPY_SOURCE_HEADER, source);

            if (sourceAccessCondition != null) {
                sourceAccessCondition.applySourceConditionToRequest(request);
            }

            if (destinationAccessCondition != null) {
                destinationAccessCondition.applyConditionToRequest(request);
            }

            return request;
        }


        public static HttpURLConnection createContainer(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext) throws IOException, URISyntaxException, StorageException {
            final UriQueryBuilder containerBuilder = getContainerUriQueryBuilder();
            return BaseRequest.create(uri, blobOptions, containerBuilder, opContext);
        }


        private static HttpURLConnection createURLConnection(final URI uri, final UriQueryBuilder query,
                final BlobRequestOptions blobOptions, final OperationContext opContext) throws IOException,
                URISyntaxException, StorageException {
            return BaseRequest.createURLConnection(uri, blobOptions, query, opContext);
        }


        public static HttpURLConnection deleteBlob(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition, final String snapshotVersion,
                final DeleteSnapshotsOption deleteSnapshotsOption) throws IOException, URISyntaxException, StorageException {

            if (snapshotVersion != null && deleteSnapshotsOption != DeleteSnapshotsOption.NONE) {
                throw new IllegalArgumentException(String.format(SR.DELETE_SNAPSHOT_NOT_VALID_ERROR,
                        "deleteSnapshotsOption", "snapshot"));
            }

            final UriQueryBuilder builder = new UriQueryBuilder();
            BlobRequest.addSnapshot(builder, snapshotVersion);
            final HttpURLConnection request = BaseRequest.delete(uri, blobOptions, builder, opContext);

            if (accessCondition != null) {
                accessCondition.applyConditionToRequest(request);
            }

            switch (deleteSnapshotsOption) {
                case NONE:
                    // nop
                    break;
                case INCLUDE_SNAPSHOTS:
                    request.setRequestProperty(Constants.HeaderConstants.DELETE_SNAPSHOT_HEADER,
                            BlobConstants.INCLUDE_SNAPSHOTS_VALUE);
                    break;
                case DELETE_SNAPSHOTS_ONLY:
                    request.setRequestProperty(Constants.HeaderConstants.DELETE_SNAPSHOT_HEADER,
                            BlobConstants.SNAPSHOTS_ONLY_VALUE);
                    break;
                default:
                    break;
            }

            return request;
        }


        public static HttpURLConnection deleteContainer(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition) throws IOException,
                URISyntaxException, StorageException {
            final UriQueryBuilder containerBuilder = getContainerUriQueryBuilder();
            HttpURLConnection request = BaseRequest.delete(uri, blobOptions, containerBuilder, opContext);
            if (accessCondition != null) {
                accessCondition.applyConditionToRequest(request);
            }

            return request;
        }


        public static HttpURLConnection getAcl(final URI uri, final BlobRequestOptions blobOptions,
                final AccessCondition accessCondition, final OperationContext opContext) throws IOException,
                URISyntaxException, StorageException {
            final UriQueryBuilder builder = getContainerUriQueryBuilder();
            builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.ACL);

            final HttpURLConnection request = createURLConnection(uri, builder, blobOptions, opContext);

            request.setRequestMethod(Constants.HTTP_GET);

            if (accessCondition != null) {
                accessCondition.applyLeaseConditionToRequest(request);
            }

            return request;
        }


        public static HttpURLConnection getBlob(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition, final String snapshotVersion,
                final Long offset, final Long count, boolean requestRangeContentMD5) throws IOException,
                URISyntaxException, StorageException {

            if (offset != null && requestRangeContentMD5) {
                Utility.assertNotNull("count", count);
                Utility.assertInBounds("count", count, 1, Constants.MAX_BLOCK_SIZE);
            }

            final UriQueryBuilder builder = new UriQueryBuilder();
            BlobRequest.addSnapshot(builder, snapshotVersion);
            final HttpURLConnection request = BaseRequest.createURLConnection(uri, blobOptions, builder, opContext);
            request.setRequestMethod(Constants.HTTP_GET);

            if (accessCondition != null) {
                accessCondition.applyConditionToRequest(request);
            }

            if (offset != null) {
                long rangeStart = offset;
                long rangeEnd;
                if (count != null) {
                    rangeEnd = offset + count - 1;
                    request.setRequestProperty(Constants.HeaderConstants.STORAGE_RANGE_HEADER, String.format(
                            Utility.LOCALE_US, Constants.HeaderConstants.RANGE_HEADER_FORMAT, rangeStart, rangeEnd));
                }
                else {
                    request.setRequestProperty(Constants.HeaderConstants.STORAGE_RANGE_HEADER, String.format(
                            Utility.LOCALE_US, Constants.HeaderConstants.BEGIN_RANGE_HEADER_FORMAT, rangeStart));
                }
            }

            if (offset != null && requestRangeContentMD5) {
                request.setRequestProperty(Constants.HeaderConstants.RANGE_GET_CONTENT_MD5, Constants.TRUE);
            }

            return request;
        }


        public static HttpURLConnection getBlobProperties(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition, final String snapshotVersion)
                throws StorageException, IOException, URISyntaxException {
            final UriQueryBuilder builder = new UriQueryBuilder();
            BlobRequest.addSnapshot(builder, snapshotVersion);
            return getProperties(uri, blobOptions, opContext, accessCondition, builder);
        }


        public static HttpURLConnection getBlockList(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition, final String snapshotVersion,
                final BlockListingFilter blockFilter) throws StorageException, IOException, URISyntaxException {

            final UriQueryBuilder builder = new UriQueryBuilder();

            builder.add(Constants.QueryConstants.COMPONENT, BLOCK_LIST_QUERY_ELEMENT_NAME);
            builder.add(BLOCK_LIST_TYPE_QUERY_ELEMENT_NAME, blockFilter.toString());
            BlobRequest.addSnapshot(builder, snapshotVersion);

            final HttpURLConnection request = BaseRequest.createURLConnection(uri, blobOptions, builder, opContext);
            request.setRequestMethod(Constants.HTTP_GET);

            if (accessCondition != null) {
                accessCondition.applyConditionToRequest(request);
            }

            return request;
        }


        public static HttpURLConnection getContainerProperties(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, AccessCondition accessCondition) throws IOException, URISyntaxException,
                StorageException {
            final UriQueryBuilder containerBuilder = getContainerUriQueryBuilder();
            return getProperties(uri, blobOptions, opContext, accessCondition, containerBuilder);
        }


        private static UriQueryBuilder getContainerUriQueryBuilder() throws StorageException {
            final UriQueryBuilder uriBuilder = new UriQueryBuilder();
            try {
                uriBuilder.add(Constants.QueryConstants.RESOURCETYPE, "container");
            }
            catch (final IllegalArgumentException e) {
                throw Utility.generateNewUnexpectedStorageException(e);
            }
            return uriBuilder;
        }


        public static HttpURLConnection getPageRanges(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition, final String snapshotVersion)
                throws StorageException, IOException, URISyntaxException {

            final UriQueryBuilder builder = new UriQueryBuilder();
            builder.add(Constants.QueryConstants.COMPONENT, PAGE_LIST_QUERY_ELEMENT_NAME);
            BlobRequest.addSnapshot(builder, snapshotVersion);

            final HttpURLConnection request = createURLConnection(uri, builder, blobOptions, opContext);
            request.setRequestMethod(Constants.HTTP_GET);

            if (accessCondition != null) {
                accessCondition.applyConditionToRequest(request);
            }

            BaseRequest.addOptionalHeader(request, BlobConstants.SNAPSHOT, snapshotVersion);
            return request;
        }


        private static HttpURLConnection getProperties(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, AccessCondition accessCondition, final UriQueryBuilder builder)
                throws IOException, URISyntaxException, StorageException {
            HttpURLConnection request = BaseRequest.getProperties(uri, blobOptions, builder, opContext);

            if (accessCondition != null) {
                accessCondition.applyLeaseConditionToRequest(request);
            }

            return request;
        }


        private static HttpURLConnection lease(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition, final LeaseAction action,
                final Integer leaseTimeInSeconds, final String proposedLeaseId, final Integer breakPeriodInSeconds,
                final UriQueryBuilder builder) throws IOException, URISyntaxException, StorageException {
            final HttpURLConnection request = createURLConnection(uri, builder, blobOptions, opContext);

            request.setDoOutput(true);
            request.setRequestMethod(Constants.HTTP_PUT);
            request.setFixedLengthStreamingMode(0);
            request.setRequestProperty(Constants.HeaderConstants.LEASE_ACTION_HEADER, action.toString());

            request.setRequestProperty(Constants.HeaderConstants.LEASE_DURATION, leaseTimeInSeconds == null ? "-1"
                    : leaseTimeInSeconds.toString());

            if (proposedLeaseId != null) {
                request.setRequestProperty(Constants.HeaderConstants.PROPOSED_LEASE_ID_HEADER, proposedLeaseId);
            }

            if (breakPeriodInSeconds != null) {
                request.setRequestProperty(Constants.HeaderConstants.LEASE_BREAK_PERIOD_HEADER, breakPeriodInSeconds.toString());
            }

            if (accessCondition != null) {
                accessCondition.applyConditionToRequest(request);
            }
            return request;
        }


        public static HttpURLConnection leaseBlob(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition, final LeaseAction action,
                final Integer leaseTimeInSeconds, final String proposedLeaseId, final Integer breakPeriodInSeconds)
                throws IOException, URISyntaxException, StorageException {
            UriQueryBuilder builder = new UriQueryBuilder();
            builder.add(Constants.QueryConstants.COMPONENT, BlobConstants.LEASE);

            return lease(uri, blobOptions, opContext, accessCondition, action, leaseTimeInSeconds, proposedLeaseId,
                    breakPeriodInSeconds, builder);
        }


        public static HttpURLConnection leaseContainer(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition, final LeaseAction action,
                final Integer leaseTimeInSeconds, final String proposedLeaseId, final Integer breakPeriodInSeconds)
                throws IOException, URISyntaxException, StorageException {

            final UriQueryBuilder builder = getContainerUriQueryBuilder();
            builder.add(Constants.QueryConstants.COMPONENT, BlobConstants.LEASE);

            return lease(uri, blobOptions, opContext, accessCondition, action, leaseTimeInSeconds, proposedLeaseId,
                    breakPeriodInSeconds, builder);
        }


        public static HttpURLConnection listBlobs(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final BlobListingContext listingContext) throws URISyntaxException,
                IOException, StorageException {

            final UriQueryBuilder builder = getContainerUriQueryBuilder();
            builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.LIST);

            if (listingContext != null) {
                if (!Utility.isNullOrEmpty(listingContext.getPrefix())) {
                    builder.add(Constants.QueryConstants.PREFIX, listingContext.getPrefix());
                }

                if (!Utility.isNullOrEmpty(listingContext.getDelimiter())) {
                    builder.add(Constants.QueryConstants.DELIMITER, listingContext.getDelimiter());
                }

                if (!Utility.isNullOrEmpty(listingContext.getMarker())) {
                    builder.add(Constants.QueryConstants.MARKER, listingContext.getMarker());
                }

                if (listingContext.getMaxResults() != null && listingContext.getMaxResults() > 0) {
                    builder.add(Constants.QueryConstants.MAX_RESULTS, listingContext.getMaxResults().toString());
                }

                if (listingContext.getListingDetails() != null && listingContext.getListingDetails().size() > 0) {
                    final StringBuilder sb = new StringBuilder();

                    boolean started = false;

                    if (listingContext.getListingDetails().contains(BlobListingDetails.SNAPSHOTS)) {
                        if (!started) {
                            started = true;
                        }
                        else {
                            sb.append(",");
                        }

                        sb.append(SNAPSHOTS_QUERY_ELEMENT_NAME);
                    }

                    if (listingContext.getListingDetails().contains(BlobListingDetails.UNCOMMITTED_BLOBS)) {
                        if (!started) {
                            started = true;
                        }
                        else {
                            sb.append(",");
                        }

                        sb.append(UNCOMMITTED_BLOBS_QUERY_ELEMENT_NAME);
                    }

                    if (listingContext.getListingDetails().contains(BlobListingDetails.COPY)) {
                        if (!started) {
                            started = true;
                        }
                        else {
                            sb.append(",");
                        }

                        sb.append(COPY_QUERY_ELEMENT_NAME);
                    }

                    if (listingContext.getListingDetails().contains(BlobListingDetails.METADATA)) {
                        if (!started) {
                            started = true;
                        }
                        else {
                            sb.append(",");
                        }

                        sb.append(Constants.QueryConstants.METADATA);
                    }

                    builder.add(Constants.QueryConstants.INCLUDE, sb.toString());
                }
            }

            final HttpURLConnection request = createURLConnection(uri, builder, blobOptions, opContext);

            request.setRequestMethod(Constants.HTTP_GET);

            return request;
        }


        public static HttpURLConnection listContainers(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final ListingContext listingContext,
                final ContainerListingDetails detailsIncluded) throws URISyntaxException, IOException, StorageException {
            final UriQueryBuilder builder = BaseRequest.getListUriQueryBuilder(listingContext);

            if (detailsIncluded == ContainerListingDetails.ALL || detailsIncluded == ContainerListingDetails.METADATA) {
                builder.add(Constants.QueryConstants.INCLUDE, Constants.QueryConstants.METADATA);
            }

            final HttpURLConnection request = createURLConnection(uri, builder, blobOptions, opContext);

            request.setRequestMethod(Constants.HTTP_GET);

            return request;
        }


        public static HttpURLConnection putBlob(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition, final BlobProperties properties,
                final BlobType blobType, final long pageBlobSize) throws IOException, URISyntaxException, StorageException {
            if (blobType == BlobType.UNSPECIFIED) {
                throw new IllegalArgumentException(SR.BLOB_TYPE_NOT_DEFINED);
            }

            final HttpURLConnection request = createURLConnection(uri, null, blobOptions, opContext);

            request.setDoOutput(true);

            request.setRequestMethod(Constants.HTTP_PUT);

            addProperties(request, properties);

            if (blobType == BlobType.PAGE_BLOB) {
                request.setFixedLengthStreamingMode(0);
                request.setRequestProperty(Constants.HeaderConstants.CONTENT_LENGTH, "0");

                request.setRequestProperty(BlobConstants.BLOB_TYPE_HEADER, BlobConstants.PAGE_BLOB);
                request.setRequestProperty(BlobConstants.SIZE, String.valueOf(pageBlobSize));

                properties.setLength(pageBlobSize);
            }
            else {
                request.setRequestProperty(BlobConstants.BLOB_TYPE_HEADER, BlobConstants.BLOCK_BLOB);
            }

            if (accessCondition != null) {
                accessCondition.applyConditionToRequest(request);
            }

            return request;
        }


        public static HttpURLConnection putBlock(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition, final String blockId)
                throws IOException, URISyntaxException, StorageException {
            final UriQueryBuilder builder = new UriQueryBuilder();
            builder.add(Constants.QueryConstants.COMPONENT, BLOCK_QUERY_ELEMENT_NAME);
            builder.add(BLOCK_ID_QUERY_ELEMENT_NAME, blockId);

            final HttpURLConnection request = createURLConnection(uri, builder, blobOptions, opContext);

            request.setDoOutput(true);
            request.setRequestMethod(Constants.HTTP_PUT);

            if (accessCondition != null) {
                accessCondition.applyConditionToRequest(request);
            }

            return request;
        }


        public static HttpURLConnection putBlockList(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition, final BlobProperties properties)
                throws IOException, URISyntaxException, StorageException {

            final UriQueryBuilder builder = new UriQueryBuilder();
            builder.add(Constants.QueryConstants.COMPONENT, BLOCK_LIST_QUERY_ELEMENT_NAME);

            final HttpURLConnection request = createURLConnection(uri, builder, blobOptions, opContext);

            request.setDoOutput(true);
            request.setRequestMethod(Constants.HTTP_PUT);

            if (accessCondition != null) {
                accessCondition.applyConditionToRequest(request);
            }

            addProperties(request, properties);

            return request;
        }


        public static HttpURLConnection putPage(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition, final PageRange pageRange,
                final PageOperationType operationType) throws IOException, URISyntaxException, StorageException {
            final UriQueryBuilder builder = new UriQueryBuilder();
            builder.add(Constants.QueryConstants.COMPONENT, PAGE_QUERY_ELEMENT_NAME);

            final HttpURLConnection request = createURLConnection(uri, builder, blobOptions, opContext);

            request.setDoOutput(true);
            request.setRequestMethod(Constants.HTTP_PUT);

            if (operationType == PageOperationType.CLEAR) {
                request.setFixedLengthStreamingMode(0);
            }

            // Page write is either update or clean; required
            request.setRequestProperty(BlobConstants.PAGE_WRITE, operationType.toString());
            request.setRequestProperty(Constants.HeaderConstants.STORAGE_RANGE_HEADER, pageRange.toString());

            if (accessCondition != null) {
                accessCondition.applyConditionToRequest(request);
                accessCondition.applySequenceConditionToRequest(request);
            }

            return request;
        }


        public static HttpURLConnection resize(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition, final Long newBlobSize)
                throws IOException, URISyntaxException, StorageException {
            final UriQueryBuilder builder = new UriQueryBuilder();
            builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.PROPERTIES);

            final HttpURLConnection request = createURLConnection(uri, builder, blobOptions, opContext);

            request.setFixedLengthStreamingMode(0);
            request.setDoOutput(true);
            request.setRequestMethod(Constants.HTTP_PUT);

            if (accessCondition != null) {
                accessCondition.applyConditionToRequest(request);
            }

            if (newBlobSize != null) {
                request.setRequestProperty(BlobConstants.SIZE, newBlobSize.toString());
            }

            return request;
        }


        public static HttpURLConnection setAcl(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition,
                final BlobContainerPublicAccessType publicAccess) throws IOException, URISyntaxException, StorageException {
            final UriQueryBuilder builder = getContainerUriQueryBuilder();
            builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.ACL);

            final HttpURLConnection request = createURLConnection(uri, builder, blobOptions, opContext);

            request.setRequestMethod(Constants.HTTP_PUT);
            request.setDoOutput(true);

            if (publicAccess != BlobContainerPublicAccessType.OFF) {
                request.setRequestProperty(BlobConstants.BLOB_PUBLIC_ACCESS_HEADER, publicAccess.toString().toLowerCase());
            }

            if (accessCondition != null) {
                accessCondition.applyLeaseConditionToRequest(request);
            }

            return request;
        }


        public static HttpURLConnection setBlobMetadata(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition) throws IOException,
                URISyntaxException, StorageException {
            return setMetadata(uri, blobOptions, opContext, accessCondition, null);
        }


        public static HttpURLConnection setBlobProperties(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition, final BlobProperties properties)
                throws IOException, URISyntaxException, StorageException {
            final UriQueryBuilder builder = new UriQueryBuilder();
            builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.PROPERTIES);

            final HttpURLConnection request = createURLConnection(uri, builder, blobOptions, opContext);

            request.setFixedLengthStreamingMode(0);
            request.setDoOutput(true);
            request.setRequestMethod(Constants.HTTP_PUT);

            if (accessCondition != null) {
                accessCondition.applyConditionToRequest(request);
            }

            if (properties != null) {
                addProperties(request, properties);
            }

            return request;
        }


        public static HttpURLConnection setContainerMetadata(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition) throws IOException,
                URISyntaxException, StorageException {
            final UriQueryBuilder containerBuilder = getContainerUriQueryBuilder();
            return setMetadata(uri, blobOptions, opContext, accessCondition, containerBuilder);
        }


        private static HttpURLConnection setMetadata(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition, final UriQueryBuilder builder)
                throws IOException, URISyntaxException, StorageException {
            final HttpURLConnection request = BaseRequest.setMetadata(uri, blobOptions, builder, opContext);

            if (accessCondition != null) {
                accessCondition.applyConditionToRequest(request);
            }

            return request;
        }


        public static HttpURLConnection snapshot(final URI uri, final BlobRequestOptions blobOptions,
                final OperationContext opContext, final AccessCondition accessCondition) throws IOException,
                URISyntaxException, StorageException {
            final UriQueryBuilder builder = new UriQueryBuilder();
            builder.add(Constants.QueryConstants.COMPONENT, BlobConstants.SNAPSHOT);
            final HttpURLConnection request = createURLConnection(uri, builder, blobOptions, opContext);

            request.setFixedLengthStreamingMode(0);
            request.setDoOutput(true);
            request.setRequestMethod(Constants.HTTP_PUT);

            if (accessCondition != null) {
                accessCondition.applyConditionToRequest(request);
            }

            return request;
        }


        private BlobRequest() {
            // No op
        }
    }

    public static final class CloudPageBlob extends CloudBlob {

        public CloudPageBlob(final URI blobAbsoluteUri) throws StorageException {
            this(new StorageUri(blobAbsoluteUri));
        }


        public CloudPageBlob(final StorageUri blobAbsoluteUri) throws StorageException {
            super(BlobType.PAGE_BLOB);

            Utility.assertNotNull("blobAbsoluteUri", blobAbsoluteUri);
            this.setStorageUri(blobAbsoluteUri);
            this.parseURIQueryStringAndVerify(blobAbsoluteUri, null,
                    Utility.determinePathStyleFromUri(blobAbsoluteUri.getPrimaryUri()));;
        }


        public CloudPageBlob(final CloudPageBlob otherBlob) {
            super(otherBlob);
        }


        public CloudPageBlob(final URI blobAbsoluteUri, final CloudBlobClient client) throws StorageException {
            this(new StorageUri(blobAbsoluteUri), client);
        }


        public CloudPageBlob(final StorageUri blobAbsoluteUri, final CloudBlobClient client) throws StorageException {
            super(BlobType.PAGE_BLOB, blobAbsoluteUri, client);
        }


        public CloudPageBlob(final URI blobAbsoluteUri, final CloudBlobClient client, final CloudBlobContainer container)
                throws StorageException {
            this(new StorageUri(blobAbsoluteUri), client, container);
        }


        public CloudPageBlob(final StorageUri blobAbsoluteUri, final CloudBlobClient client,
                final CloudBlobContainer container) throws StorageException {
            super(BlobType.PAGE_BLOB, blobAbsoluteUri, client, container);
        }


        public CloudPageBlob(final URI blobAbsoluteUri, final String snapshotID, final CloudBlobClient client)
                throws StorageException {
            this(new StorageUri(blobAbsoluteUri), snapshotID, client);
        }


        public CloudPageBlob(final StorageUri blobAbsoluteUri, final String snapshotID, final CloudBlobClient client)
                throws StorageException {
            super(BlobType.PAGE_BLOB, blobAbsoluteUri, snapshotID, client);
        }


        @DoesServiceRequest
        public void clearPages(final long offset, final long length) throws StorageException {
            this.clearPages(offset, length, null , null , null );
        }


        @DoesServiceRequest
        public void clearPages(final long offset, final long length, final AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException {
            if (offset % Constants.PAGE_SIZE != 0) {
                throw new IllegalArgumentException(SR.INVALID_PAGE_START_OFFSET);
            }

            if (length % Constants.PAGE_SIZE != 0) {
                throw new IllegalArgumentException(SR.INVALID_PAGE_BLOB_LENGTH);
            }

            if (opContext == null) {
                opContext = new OperationContext();
            }

            options = BlobRequestOptions.applyDefaults(options, BlobType.PAGE_BLOB, this.blobServiceClient);
            PageRange range = new PageRange(offset, offset + length - 1);

            this.putPagesInternal(range, PageOperationType.CLEAR, null, length, null, accessCondition, options, opContext);
        }


        @DoesServiceRequest
        public void create(final long length) throws StorageException {
            this.create(length, null , null , null );
        }


        @DoesServiceRequest
        public void create(final long length, final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            assertNoWriteOperationForSnapshot();

            if (length % Constants.PAGE_SIZE != 0) {
                throw new IllegalArgumentException(SR.INVALID_PAGE_BLOB_LENGTH);
            }

            if (opContext == null) {
                opContext = new OperationContext();
            }

            options = BlobRequestOptions.applyDefaults(options, BlobType.PAGE_BLOB, this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.createImpl(length, accessCondition, options), options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, Void> createImpl(final long length,
                final AccessCondition accessCondition, final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlob, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, Void>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    return BlobRequest.putBlob(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                            options, context, accessCondition, blob.properties, BlobType.PAGE_BLOB, length);
                }

                @Override
                public void setHeaders(HttpURLConnection connection, CloudBlob blob, OperationContext context) {
                    BlobRequest.addMetadata(connection, blob.metadata, context);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);
                }

                @Override
                public Void preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_CREATED) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    blob.updateEtagAndLastModifiedFromResponse(this.getConnection());
                    blob.getProperties().setLength(length);
                    return null;
                }

            };

            return putRequest;
        }


        @DoesServiceRequest
        public ArrayList<PageRange> downloadPageRanges() throws StorageException {
            return this.downloadPageRanges(null , null , null );
        }


        @DoesServiceRequest
        public ArrayList<PageRange> downloadPageRanges(final AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {

            if (opContext == null) {
                opContext = new OperationContext();
            }

            options = BlobRequestOptions.applyDefaults(options, BlobType.PAGE_BLOB, this.blobServiceClient);

            return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    this.downloadPageRangesImpl(accessCondition, options), options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudBlob, ArrayList<PageRange>> downloadPageRangesImpl(
                final AccessCondition accessCondition, final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudBlob, ArrayList<PageRange>> getRequest = new StorageRequest<CloudBlobClient, CloudBlob, ArrayList<PageRange>>(
                    options, this.getStorageUri()) {

                @Override
                public void setRequestLocationMode() {
                    this.setRequestLocationMode(RequestLocationMode.PRIMARY_OR_SECONDARY);
                }

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    return BlobRequest.getPageRanges(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                            options, context, accessCondition, blob.snapshotID);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, -1L, null);
                }

                @Override
                public ArrayList<PageRange> preProcessResponse(CloudBlob parentObject, CloudBlobClient client,
                        OperationContext context) throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                    }

                    return null;
                }

                @Override
                public ArrayList<PageRange> postProcessResponse(HttpURLConnection connection, CloudBlob blob,
                        CloudBlobClient client, OperationContext context, ArrayList<PageRange> storageObject)
                        throws Exception {
                    blob.updateEtagAndLastModifiedFromResponse(this.getConnection());
                    blob.updateLengthFromResponse(this.getConnection());

                    return PageRangeHandler.getPageRanges(this.getConnection().getInputStream());
                }

            };

            return getRequest;
        }


        @DoesServiceRequest
        public BlobOutputStream openWriteExisting() throws StorageException {
            return this
                    .openOutputStreamInternal(null , null , null , null );
        }


        @DoesServiceRequest
        public BlobOutputStream openWriteExisting(AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            return this
                    .openOutputStreamInternal(null , null , null , null );
        }


        @DoesServiceRequest
        public BlobOutputStream openWriteNew(final long length) throws StorageException {
            return this
                    .openOutputStreamInternal(length, null , null , null );
        }


        @DoesServiceRequest
        public BlobOutputStream openWriteNew(final long length, AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException {
            return openOutputStreamInternal(length, accessCondition, options, opContext);
        }


        private BlobOutputStream openOutputStreamInternal(Long length, AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            assertNoWriteOperationForSnapshot();

            options = BlobRequestOptions.applyDefaults(options, BlobType.PAGE_BLOB, this.blobServiceClient,
                    false);

            if (options.getStoreBlobContentMD5()) {
                throw new IllegalArgumentException(SR.BLOB_MD5_NOT_SUPPORTED_FOR_PAGE_BLOBS);
            }

            if (length != null) {
                if (length % Constants.PAGE_SIZE != 0) {
                    throw new IllegalArgumentException(SR.INVALID_PAGE_BLOB_LENGTH);
                }

                this.create(length, accessCondition, options, opContext);
            }
            else {
                this.downloadAttributes(accessCondition, options, opContext);
                length = this.getProperties().getLength();
            }

            if (accessCondition != null) {
                accessCondition = AccessCondition.generateLeaseCondition(accessCondition.getLeaseID());
            }

            return new BlobOutputStream(this, length, accessCondition, options, opContext);
        }


        @DoesServiceRequest
        private void putPagesInternal(final PageRange pageRange, final PageOperationType operationType, final byte[] data,
                final long length, final String md5, final AccessCondition accessCondition,
                final BlobRequestOptions options, final OperationContext opContext) throws StorageException {
            ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                    putPagesImpl(pageRange, operationType, data, length, md5, accessCondition, options, opContext),
                    options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudPageBlob, Void> putPagesImpl(final PageRange pageRange,
                final PageOperationType operationType, final byte[] data, final long length, final String md5,
                final AccessCondition accessCondition, final BlobRequestOptions options, final OperationContext opContext) {
            final StorageRequest<CloudBlobClient, CloudPageBlob, Void> putRequest = new StorageRequest<CloudBlobClient, CloudPageBlob, Void>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudPageBlob blob, OperationContext context)
                        throws Exception {
                    if (operationType == PageOperationType.UPDATE) {
                        this.setSendStream(new ByteArrayInputStream(data));
                        this.setLength(length);
                    }

                    return BlobRequest.putPage(blob.getTransformedAddress(opContext).getUri(this.getCurrentLocation()),
                            options, opContext, accessCondition, pageRange, operationType);
                }

                @Override
                public void setHeaders(HttpURLConnection connection, CloudPageBlob blob, OperationContext context) {
                    if (operationType == PageOperationType.UPDATE) {
                        if (options.getUseTransactionalContentMD5()) {
                            connection.setRequestProperty(Constants.HeaderConstants.CONTENT_MD5, md5);
                        }
                    }
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (operationType == PageOperationType.UPDATE) {
                        StorageRequest.signBlobQueueAndFileRequest(connection, client, length, null);
                    }
                    else {
                        StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);
                    }
                }

                @Override
                public Void preProcessResponse(CloudPageBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_CREATED) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    blob.updateEtagAndLastModifiedFromResponse(this.getConnection());
                    blob.updateSequenceNumberFromResponse(this.getConnection());
                    return null;
                }
            };

            return putRequest;
        }

        protected void updateSequenceNumberFromResponse(HttpURLConnection request) {
            final String sequenceNumber = request.getHeaderField(Constants.HeaderConstants.BLOB_SEQUENCE_NUMBER);
            if (!Utility.isNullOrEmpty(sequenceNumber)) {
                this.getProperties().setPageBlobSequenceNumber(Long.parseLong(sequenceNumber));
            }
        }


        public void resize(long size) throws StorageException {
            this.resize(size, null , null , null );
        }


        public void resize(long size, AccessCondition accessCondition, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            assertNoWriteOperationForSnapshot();

            if (size % Constants.PAGE_SIZE != 0) {
                throw new IllegalArgumentException(SR.INVALID_PAGE_BLOB_LENGTH);
            }

            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

            ExecutionEngine.executeWithRetry(this.blobServiceClient, this, this.resizeImpl(size, accessCondition, options),
                    options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, CloudPageBlob, Void> resizeImpl(final long size,
                final AccessCondition accessCondition, final BlobRequestOptions options) {
            final StorageRequest<CloudBlobClient, CloudPageBlob, Void> putRequest = new StorageRequest<CloudBlobClient, CloudPageBlob, Void>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudPageBlob blob, OperationContext context)
                        throws Exception {
                    return BlobRequest.resize(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                            options, context, accessCondition, size);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, null);
                }

                @Override
                public Void preProcessResponse(CloudPageBlob blob, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                        return null;
                    }

                    blob.getProperties().setLength(size);
                    blob.updateEtagAndLastModifiedFromResponse(this.getConnection());
                    blob.updateSequenceNumberFromResponse(this.getConnection());
                    return null;
                }
            };

            return putRequest;
        }


        @Override
        @DoesServiceRequest
        public void upload(final InputStream sourceStream, final long length) throws StorageException, IOException {
            this.upload(sourceStream, length, null , null , null );
        }


        @Override
        @DoesServiceRequest
        public void upload(final InputStream sourceStream, final long length, final AccessCondition accessCondition,
                BlobRequestOptions options, OperationContext opContext) throws StorageException, IOException {
            assertNoWriteOperationForSnapshot();

            if (opContext == null) {
                opContext = new OperationContext();
            }

            options = BlobRequestOptions.applyDefaults(options, BlobType.PAGE_BLOB, this.blobServiceClient);

            if (length <= 0 || length % Constants.PAGE_SIZE != 0) {
                throw new IllegalArgumentException(SR.INVALID_PAGE_BLOB_LENGTH);
            }

            if (options.getStoreBlobContentMD5()) {
                throw new IllegalArgumentException(SR.BLOB_MD5_NOT_SUPPORTED_FOR_PAGE_BLOBS);
            }

            if (sourceStream.markSupported()) {
                // Mark sourceStream for current position.
                sourceStream.mark(Constants.MAX_MARK_LENGTH);
            }

            final BlobOutputStream streamRef = this.openWriteNew(length, accessCondition, options, opContext);
            try {
                streamRef.write(sourceStream, length);
            }
            finally {
                streamRef.close();
            }
        }


        @DoesServiceRequest
        public void uploadPages(final InputStream sourceStream, final long offset, final long length)
                throws StorageException, IOException {
            this.uploadPages(sourceStream, offset, length, null , null , null );
        }


        @DoesServiceRequest
        public void uploadPages(final InputStream sourceStream, final long offset, final long length,
                final AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
                throws StorageException, IOException {

            if (offset % Constants.PAGE_SIZE != 0) {
                throw new IllegalArgumentException(SR.INVALID_PAGE_START_OFFSET);
            }

            if (length == 0 || length % Constants.PAGE_SIZE != 0) {
                throw new IllegalArgumentException(SR.INVALID_PAGE_BLOB_LENGTH);
            }

            if (length > 4 * Constants.MB) {
                throw new IllegalArgumentException(SR.INVALID_MAX_WRITE_SIZE);
            }

            assertNoWriteOperationForSnapshot();

            if (opContext == null) {
                opContext = new OperationContext();
            }

            options = BlobRequestOptions.applyDefaults(options, BlobType.PAGE_BLOB, this.blobServiceClient);

            final PageRange pageRange = new PageRange(offset, offset + length - 1);
            final byte[] data = new byte[(int) length];
            String md5 = null;

            int count = 0;
            int total = 0;
            while (total < length) {
                count = sourceStream.read(data, total, (int) Math.min(length - total, Integer.MAX_VALUE));
                total += count;
            }

            if (options.getUseTransactionalContentMD5()) {
                try {
                    final MessageDigest digest = MessageDigest.getInstance("MD5");
                    digest.update(data, 0, data.length);
                    md5 = Base64.encode(digest.digest());
                }
                catch (final NoSuchAlgorithmException e) {
                    // This wont happen, throw fatal.
                    throw Utility.generateNewUnexpectedStorageException(e);
                }
            }

            this.putPagesInternal(pageRange, PageOperationType.UPDATE, data, length, md5, accessCondition, options,
                    opContext);
        }


        @Override
        public void setStreamWriteSizeInBytes(final int streamWriteSizeInBytes) {
            if (streamWriteSizeInBytes > Constants.MAX_BLOCK_SIZE || streamWriteSizeInBytes < Constants.PAGE_SIZE
                    || streamWriteSizeInBytes % Constants.PAGE_SIZE != 0) {
                throw new IllegalArgumentException("StreamWriteSizeInBytes");
            }

            this.streamWriteSizeInBytes = streamWriteSizeInBytes;
        }
    }

    static final class PageRangeHandler extends DefaultHandler {

        private final Stack<String> elementStack = new Stack<String>();
        private StringBuilder bld = new StringBuilder();

        private final ArrayList<PageRange> pages = new ArrayList<PageRange>();

        private long startOffset;
        private long endOffset;


        protected static ArrayList<PageRange> getPageRanges(InputStream streamRef) throws ParserConfigurationException,
                SAXException, IOException {
            SAXParser saxParser = Utility.getSAXParser();
            PageRangeHandler handler = new PageRangeHandler();
            saxParser.parse(streamRef, handler);

            return handler.pages;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            this.elementStack.push(localName);
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            String currentNode = this.elementStack.pop();

            // if the node popped from the stack and the localName don't match, the xml document is improperly formatted
            if (!localName.equals(currentNode)) {
                throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
            }

            String value = this.bld.toString();
            if (value.isEmpty()) {
                value = null;
            }

            if (BlobConstants.PAGE_RANGE_ELEMENT.equals(currentNode)) {
                final PageRange pageRef = new PageRange(this.startOffset, this.endOffset);
                this.pages.add(pageRef);
            }
            else if (BlobConstants.START_ELEMENT.equals(currentNode)) {
                this.startOffset = Long.parseLong(value);
            }
            else if (Constants.END_ELEMENT.equals(currentNode)) {
                this.endOffset = Long.parseLong(value);
            }

            this.bld = new StringBuilder();
        }

        @Override
        public void characters(char ch[], int start, int length) throws SAXException {
            this.bld.append(ch, start, length);
        }
    }

    public static final class CloudBlobClient extends ServiceClient {


        private String directoryDelimiter = BlobConstants.DEFAULT_DELIMITER;


        private BlobRequestOptions defaultRequestOptions;


        public CloudBlobClient(final URI baseUri) {
            this(new StorageUri(baseUri), null );
        }


        public CloudBlobClient(final StorageUri baseUri) {
            this(baseUri, null );
        }


        public CloudBlobClient(final URI baseUri, StorageCredentials credentials) {
            this(new StorageUri(baseUri), credentials);
        }


        public CloudBlobClient(final StorageUri storageUri, StorageCredentials credentials) {
            super(storageUri, credentials);
            this.defaultRequestOptions = new BlobRequestOptions();
            this.defaultRequestOptions.setLocationMode(LocationMode.PRIMARY_ONLY);
            this.defaultRequestOptions.setRetryPolicyFactory(new RetryExponentialRetry());
            this.defaultRequestOptions.setConcurrentRequestCount(BlobConstants.DEFAULT_CONCURRENT_REQUEST_COUNT);
            this.defaultRequestOptions.setDisableContentMD5Validation(false);
            this.defaultRequestOptions
                    .setSingleBlobPutThresholdInBytes(BlobConstants.DEFAULT_SINGLE_BLOB_PUT_THRESHOLD_IN_BYTES);
            this.defaultRequestOptions.setUseTransactionalContentMD5(false);
        }


        @Deprecated
        public int getConcurrentRequestCount() {
            return this.defaultRequestOptions.getConcurrentRequestCount();
        }


        public CloudBlobContainer getContainerReference(final String containerName) throws URISyntaxException,
                StorageException {
            return new CloudBlobContainer(containerName, this);
        }


        public String getDirectoryDelimiter() {
            return this.directoryDelimiter;
        }


        @Deprecated
        public int getSingleBlobPutThresholdInBytes() {
            return this.getDefaultRequestOptions().getSingleBlobPutThresholdInBytes();
        }


        @DoesServiceRequest
        public Iterable<CloudBlobContainer> listContainers() {
            return this
                    .listContainersWithPrefix(null, ContainerListingDetails.NONE, null , null );
        }


        @DoesServiceRequest
        public Iterable<CloudBlobContainer> listContainers(final String prefix) {
            return this
                    .listContainersWithPrefix(prefix, ContainerListingDetails.NONE, null , null );
        }


        @DoesServiceRequest
        public Iterable<CloudBlobContainer> listContainers(final String prefix,
                final ContainerListingDetails detailsIncluded, final BlobRequestOptions options,
                final OperationContext opContext) {
            return this.listContainersWithPrefix(prefix, detailsIncluded, options, opContext);
        }


        @DoesServiceRequest
        public ResultSegment<CloudBlobContainer> listContainersSegmented() throws StorageException {
            return this.listContainersSegmented(null, ContainerListingDetails.NONE, null, null ,
                    null , null );
        }


        @DoesServiceRequest
        public ResultSegment<CloudBlobContainer> listContainersSegmented(final String prefix) throws StorageException {
            return this.listContainersWithPrefixSegmented(prefix, ContainerListingDetails.NONE, null,
                    null , null , null );
        }


        @DoesServiceRequest
        public ResultSegment<CloudBlobContainer> listContainersSegmented(final String prefix,
                final ContainerListingDetails detailsIncluded, final Integer maxResults,
                final ResultContinuation continuationToken, final BlobRequestOptions options,
                final OperationContext opContext) throws StorageException {

            return this.listContainersWithPrefixSegmented(prefix, detailsIncluded, maxResults, continuationToken, options,
                    opContext);
        }


        private Iterable<CloudBlobContainer> listContainersWithPrefix(final String prefix,
                final ContainerListingDetails detailsIncluded, BlobRequestOptions options, OperationContext opContext) {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this);

            SegmentedStorageRequest segmentedRequest = new SegmentedStorageRequest();

            return new LazySegmentedIterable<CloudBlobClient, Void, CloudBlobContainer>(
                    this.listContainersWithPrefixSegmentedImpl(prefix, detailsIncluded, null, options, segmentedRequest),
                    this, null, options.getRetryPolicyFactory(), opContext);
        }


        private ResultSegment<CloudBlobContainer> listContainersWithPrefixSegmented(final String prefix,
                final ContainerListingDetails detailsIncluded, final Integer maxResults,
                final ResultContinuation continuationToken, BlobRequestOptions options, OperationContext opContext)
                throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this);

            Utility.assertContinuationType(continuationToken, ResultContinuationType.CONTAINER);

            SegmentedStorageRequest segmentedRequest = new SegmentedStorageRequest();
            segmentedRequest.setToken(continuationToken);

            return ExecutionEngine.executeWithRetry(this, null, this.listContainersWithPrefixSegmentedImpl(prefix,
                    detailsIncluded, maxResults, options, segmentedRequest), options.getRetryPolicyFactory(), opContext);
        }

        private StorageRequest<CloudBlobClient, Void, ResultSegment<CloudBlobContainer>> listContainersWithPrefixSegmentedImpl(
                final String prefix, final ContainerListingDetails detailsIncluded, final Integer maxResults,
                final BlobRequestOptions options, final SegmentedStorageRequest segmentedRequest) {

            Utility.assertContinuationType(segmentedRequest.getToken(), ResultContinuationType.CONTAINER);

            final ListingContext listingContext = new ListingContext(prefix, maxResults);

            final StorageRequest<CloudBlobClient, Void, ResultSegment<CloudBlobContainer>> getRequest = new StorageRequest<CloudBlobClient, Void, ResultSegment<CloudBlobContainer>>(
                    options, this.getStorageUri()) {

                @Override
                public void setRequestLocationMode() {
                    this.setRequestLocationMode(Utility.getListingLocationMode(segmentedRequest.getToken()));
                }

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, Void parentObject, OperationContext context)
                        throws Exception {
                    listingContext.setMarker(segmentedRequest.getToken() != null ? segmentedRequest.getToken()
                            .getNextMarker() : null);
                    return BlobRequest.listContainers(
                            client.getCredentials().transformUri(client.getStorageUri()).getUri(this.getCurrentLocation()),
                            options, context, listingContext, detailsIncluded);
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, -1L, null);
                }

                @Override
                public ResultSegment<CloudBlobContainer> preProcessResponse(Void parentObject, CloudBlobClient client,
                        OperationContext context) throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                    }
                    return null;
                }

                @Override
                public ResultSegment<CloudBlobContainer> postProcessResponse(HttpURLConnection connection, Void container,
                        CloudBlobClient client, OperationContext context, ResultSegment<CloudBlobContainer> storageObject)
                        throws Exception {
                    final ListResponse<CloudBlobContainer> response = ContainerListHandler.getContainerList(this
                            .getConnection().getInputStream(), client);
                    ResultContinuation newToken = null;

                    if (response.getNextMarker() != null) {
                        newToken = new ResultContinuation();
                        newToken.setNextMarker(response.getNextMarker());
                        newToken.setContinuationType(ResultContinuationType.CONTAINER);
                        newToken.setTargetLocation(this.getResult().getTargetLocation());
                    }

                    final ResultSegment<CloudBlobContainer> resSegment = new ResultSegment<CloudBlobContainer>(
                            response.getResults(), response.getMaxResults(), newToken);

                    // Important for listContainers because this is required by the lazy iterator between executions.
                    segmentedRequest.setToken(resSegment.getContinuationToken());
                    return resSegment;
                }
            };

            return getRequest;
        }


        @DoesServiceRequest
        public ServiceStats getServiceStats() throws StorageException {
            return this.getServiceStats(null , null );
        }


        @DoesServiceRequest
        public ServiceStats getServiceStats(BlobRequestOptions options, OperationContext opContext) throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this);

            return ExecutionEngine.executeWithRetry(this, null, this.getServiceStatsImpl(options, false),
                    options.getRetryPolicyFactory(), opContext);
        }


        @DoesServiceRequest
        public final ServiceProperties downloadServiceProperties() throws StorageException {
            return this.downloadServiceProperties(null , null );
        }


        @DoesServiceRequest
        public final ServiceProperties downloadServiceProperties(BlobRequestOptions options, OperationContext opContext)
                throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this);

            return ExecutionEngine.executeWithRetry(this, null, this.downloadServicePropertiesImpl(options, false),
                    options.getRetryPolicyFactory(), opContext);
        }


        @DoesServiceRequest
        public void uploadServiceProperties(final ServiceProperties properties) throws StorageException {
            this.uploadServiceProperties(properties, null , null );
        }


        @DoesServiceRequest
        public void uploadServiceProperties(final ServiceProperties properties, BlobRequestOptions options,
                OperationContext opContext) throws StorageException {
            if (opContext == null) {
                opContext = new OperationContext();
            }

            opContext.initialize();
            options = BlobRequestOptions.applyDefaults(options, BlobType.UNSPECIFIED, this);

            Utility.assertNotNull("properties", properties);

            ExecutionEngine.executeWithRetry(this, null,
                    this.uploadServicePropertiesImpl(properties, options, opContext, false),
                    options.getRetryPolicyFactory(), opContext);
        }


        @Deprecated
        public void setConcurrentRequestCount(final int concurrentRequestCount) {
            this.defaultRequestOptions.setConcurrentRequestCount(concurrentRequestCount);
        }


        public void setDirectoryDelimiter(final String directoryDelimiter) {
            Utility.assertNotNullOrEmpty("directoryDelimiter", directoryDelimiter);
            this.directoryDelimiter = directoryDelimiter;
        }


        @Deprecated
        public void setSingleBlobPutThresholdInBytes(final int singleBlobPutThresholdInBytes) {
            this.defaultRequestOptions.setSingleBlobPutThresholdInBytes(singleBlobPutThresholdInBytes);
        }


        @Override
        public BlobRequestOptions getDefaultRequestOptions() {
            return this.defaultRequestOptions;
        }


        public void setDefaultRequestOptions(BlobRequestOptions defaultRequestOptions) {
            Utility.assertNotNull("defaultRequestOptions", defaultRequestOptions);
            this.defaultRequestOptions = defaultRequestOptions;
        }


        @Override
        protected boolean isUsePathStyleUris() {
            return super.isUsePathStyleUris();
        }
    }

    public static final class PageRange {

        private long endOffset;


        private long startOffset;


        public PageRange(final long start, final long end) {
            this.setStartOffset(start);
            this.setEndOffset(end);
        }


        public long getEndOffset() {
            return this.endOffset;
        }


        public long getStartOffset() {
            return this.startOffset;
        }


        public void setEndOffset(final long endOffset) {
            this.endOffset = endOffset;
        }


        public void setStartOffset(final long startOffset) {
            this.startOffset = startOffset;
        }


        @Override
        public String toString() {
            return String.format("bytes=%d-%d", this.getStartOffset(), this.getEndOffset());
        }
    }

    public static enum BlobType {

        UNSPECIFIED,


        BLOCK_BLOB,


        PAGE_BLOB;


        protected static BlobType parse(final String typeString) {
            if (Utility.isNullOrEmpty(typeString)) {
                return UNSPECIFIED;
            }
            else if ("blockblob".equals(typeString.toLowerCase(Locale.US))) {
                return BLOCK_BLOB;
            }
            else if ("pageblob".equals(typeString.toLowerCase(Locale.US))) {
                return PAGE_BLOB;
            }
            else {
                return UNSPECIFIED;
            }
        }
    }

    public static enum SharedAccessBlobPermissions {

        READ((byte) 0x1),


        WRITE((byte) 0x2),


        DELETE((byte) 0x4),


        LIST((byte) 0x8);


        protected static EnumSet<SharedAccessBlobPermissions> fromByte(final byte value) {
            final EnumSet<SharedAccessBlobPermissions> retSet = EnumSet.noneOf(SharedAccessBlobPermissions.class);

            if (value == READ.value) {
                retSet.add(READ);
            }

            if (value == WRITE.value) {
                retSet.add(WRITE);
            }
            if (value == DELETE.value) {
                retSet.add(DELETE);
            }
            if (value == LIST.value) {
                retSet.add(LIST);
            }

            return retSet;
        }


        private byte value;


        private SharedAccessBlobPermissions(final byte val) {
            this.value = val;
        }
    }

    public static enum DeleteSnapshotsOption {

        DELETE_SNAPSHOTS_ONLY,


        INCLUDE_SNAPSHOTS,


        NONE
    }

    static final class BlobListHandler extends DefaultHandler {

        private final Stack<String> elementStack = new Stack<String>();
        private StringBuilder bld = new StringBuilder();

        private final ListBlobsResponse response = new ListBlobsResponse();

        private final CloudBlobContainer container;

        private BlobProperties properties;
        private HashMap<String, String> metadata;
        private CopyState copyState;
        private String blobName;
        private String snapshotID;

        private BlobListHandler(CloudBlobContainer container) {
            this.container = container;
        }


        public static ListBlobsResponse getBlobList(final InputStream stream, final CloudBlobContainer container)
                throws ParserConfigurationException, SAXException, IOException {
            SAXParser saxParser = Utility.getSAXParser();
            BlobListHandler handler = new BlobListHandler(container);
            saxParser.parse(stream, handler);

            return handler.response;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            this.elementStack.push(localName);

            if (BlobConstants.BLOB_ELEMENT.equals(localName) || BlobConstants.BLOB_PREFIX_ELEMENT.equals(localName)) {
                this.blobName = Constants.EMPTY_STRING;
                this.snapshotID = null;
                this.properties = new BlobProperties();
                this.metadata = new HashMap<String, String>();
                this.copyState = null;
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            String currentNode = this.elementStack.pop();

            // if the node popped from the stack and the localName don't match, the xml document is improperly formatted
            if (!localName.equals(currentNode)) {
                throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
            }

            String parentNode = null;
            if (!this.elementStack.isEmpty()) {
                parentNode = this.elementStack.peek();
            }

            String value = this.bld.toString();
            if (value.isEmpty()) {
                value = null;
            }

            if (BlobConstants.BLOB_ELEMENT.equals(currentNode)) {
                CloudBlob retBlob = null;
                try {
                    if (this.properties.getBlobType() == BlobType.BLOCK_BLOB) {
                        retBlob = this.container.getBlockBlobReference(this.blobName);
                    }
                    else if (this.properties.getBlobType() == BlobType.PAGE_BLOB) {
                        retBlob = this.container.getPageBlobReference(this.blobName);
                    }
                    else {
                        throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
                    }
                }
                catch (StorageException e) {
                    throw new SAXException(e);
                }
                catch (URISyntaxException e) {
                    throw new SAXException(e);
                }

                retBlob.snapshotID = this.snapshotID;
                retBlob.properties = this.properties;
                retBlob.metadata = this.metadata;
                retBlob.properties.setCopyState(this.copyState);

                this.response.getResults().add(retBlob);
            }
            else if (BlobConstants.BLOB_PREFIX_ELEMENT.equals(currentNode)) {
                try {
                    this.response.getResults().add(this.container.getDirectoryReference(this.blobName));
                }
                catch (URISyntaxException e) {
                    throw new SAXException(e);
                }
            }
            else if (ListResponse.ENUMERATION_RESULTS.equals(parentNode)) {
                if (Constants.PREFIX_ELEMENT.equals(currentNode)) {
                    this.response.setPrefix(value);
                }
                else if (Constants.MARKER_ELEMENT.equals(currentNode)) {
                    this.response.setMarker(value);
                }
                else if (Constants.NEXT_MARKER_ELEMENT.equals(currentNode)) {
                    this.response.setNextMarker(value);
                }
                else if (Constants.MAX_RESULTS_ELEMENT.equals(currentNode)) {
                    this.response.setMaxResults(Integer.parseInt(value));
                }
                else if (Constants.DELIMITER_ELEMENT.equals(currentNode)) {
                    this.response.setDelimiter(value);
                }
            }
            else if (BlobConstants.BLOB_ELEMENT.equals(parentNode)) {
                if (Constants.NAME_ELEMENT.equals(currentNode)) {
                    this.blobName = value;
                }
                else if (BlobConstants.SNAPSHOT_ELEMENT.equals(currentNode)) {
                    this.snapshotID = value;
                }
            }
            else if (BlobConstants.BLOB_PREFIX_ELEMENT.equals(parentNode)) {
                // Blob or BlobPrefix
                if (Constants.NAME_ELEMENT.equals(currentNode)) {
                    this.blobName = value;
                }
            }
            else if (Constants.PROPERTIES.equals(parentNode)) {
                try {
                    this.setProperties(currentNode, value);
                }
                catch (ParseException e) {
                    throw new SAXException(e);
                }
                catch (URISyntaxException e) {
                    throw new SAXException(e);
                }
            }
            else if (Constants.METADATA_ELEMENT.equals(parentNode)) {
                this.metadata.put(currentNode, value);
            }

            this.bld = new StringBuilder();
        }

        @Override
        public void characters(char ch[], int start, int length) throws SAXException {
            this.bld.append(ch, start, length);
        }

        private void setProperties(String currentNode, String value) throws ParseException, URISyntaxException,
                SAXException {

            if (Constants.LAST_MODIFIED_ELEMENT.equals(currentNode)) {
                this.properties.setLastModified(Utility.parseRFC1123DateFromStringInGMT(value));
            }
            else if (Constants.ETAG_ELEMENT.equals(currentNode)) {
                this.properties.setEtag(Utility.formatETag(value));
            }
            else if (Constants.HeaderConstants.CONTENT_LENGTH.equals(currentNode)) {
                this.properties.setLength(Long.parseLong(value));
            }
            else if (Constants.HeaderConstants.CONTENT_TYPE.equals(currentNode)) {
                this.properties.setContentType(value);
            }
            else if (Constants.HeaderConstants.CONTENT_ENCODING.equals(currentNode)) {
                this.properties.setContentEncoding(value);
            }
            else if (Constants.HeaderConstants.CONTENT_LANGUAGE.equals(currentNode)) {
                this.properties.setContentLanguage(value);
            }
            else if (Constants.HeaderConstants.CONTENT_MD5.equals(currentNode)) {
                this.properties.setContentMD5(value);
            }
            else if (Constants.HeaderConstants.CACHE_CONTROL.equals(currentNode)) {
                this.properties.setCacheControl(value);
            }
            else if (Constants.HeaderConstants.CONTENT_DISPOSITION.equals(currentNode)) {
                this.properties.setContentDisposition(value);
            }
            else if (BlobConstants.BLOB_TYPE_ELEMENT.equals(currentNode)) {
                final String tempString = value;
                if (tempString.equals(BlobConstants.BLOCK_BLOB_VALUE)) {
                    this.properties.setBlobType(BlobType.BLOCK_BLOB);
                }
                else if (tempString.equals(BlobConstants.PAGE_BLOB_VALUE.toString())) {
                    this.properties.setBlobType(BlobType.PAGE_BLOB);
                }
                else {
                    throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
                }
            }
            else if (Constants.LEASE_STATUS_ELEMENT.equals(currentNode)) {
                final LeaseStatus tempStatus = LeaseStatus.parse(value);
                if (!tempStatus.equals(LeaseStatus.UNSPECIFIED)) {
                    this.properties.setLeaseStatus(tempStatus);
                }
                else {
                    throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
                }
            }
            else if (Constants.LEASE_STATE_ELEMENT.equals(currentNode)) {
                final LeaseState tempState = LeaseState.parse(value);
                if (!tempState.equals(LeaseState.UNSPECIFIED)) {
                    this.properties.setLeaseState(tempState);
                }
                else {
                    throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
                }
            }
            else if (Constants.LEASE_DURATION_ELEMENT.equals(currentNode)) {
                final LeaseDuration tempDuration = LeaseDuration.parse(value);
                if (!tempDuration.equals(LeaseDuration.UNSPECIFIED)) {
                    this.properties.setLeaseDuration(tempDuration);
                }
                else {
                    throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
                }
            }
            else if (Constants.COPY_ID_ELEMENT.equals(currentNode)) {
                if (this.copyState == null) {
                    this.copyState = new CopyState();
                }
                this.copyState.setCopyId(value);
            }
            else if (Constants.COPY_COMPLETION_TIME_ELEMENT.equals(currentNode)) {
                if (this.copyState == null) {
                    this.copyState = new CopyState();
                }
                this.copyState.setCompletionTime(Utility.parseRFC1123DateFromStringInGMT(value));
            }
            else if (Constants.COPY_STATUS_ELEMENT.equals(currentNode)) {
                if (this.copyState == null) {
                    this.copyState = new CopyState();
                }
                this.copyState.setStatus(CopyStatus.parse(value));
            }
            else if (Constants.COPY_SOURCE_ELEMENT.equals(currentNode)) {
                if (this.copyState == null) {
                    this.copyState = new CopyState();
                }
                this.copyState.setSource(new URI(value));
            }
            else if (Constants.COPY_PROGRESS_ELEMENT.equals(currentNode)) {
                if (this.copyState == null) {
                    this.copyState = new CopyState();
                }

                final String tempString = value;
                String[] progressSequence = tempString.split("/");
                this.copyState.setBytesCopied(Long.parseLong(progressSequence[0]));
                this.copyState.setTotalBytes(Long.parseLong(progressSequence[1]));
            }
            else if (Constants.COPY_STATUS_DESCRIPTION_ELEMENT.equals(currentNode)) {
                if (this.copyState == null) {
                    this.copyState = new CopyState();
                }
                this.copyState.setStatusDescription(value);
            }
        }
    }

    public static enum LeaseDuration {

        UNSPECIFIED,


        FIXED,


        INFINITE;


        protected static LeaseDuration parse(final String typeString) {
            if (Utility.isNullOrEmpty(typeString)) {
                return UNSPECIFIED;
            }
            else if ("fixed".equals(typeString.toLowerCase(Locale.US))) {
                return FIXED;
            }
            else if ("infinite".equals(typeString.toLowerCase(Locale.US))) {
                return INFINITE;
            }
            else {
                return UNSPECIFIED;
            }
        }
    }

    public static final class StorageExtendedErrorInformation implements Serializable {

        private static final long serialVersionUID = 1527013626991334677L;


        private HashMap<String, String[]> additionalDetails;


        private String errorCode;


        private String errorMessage;


        public StorageExtendedErrorInformation() {
            this.setAdditionalDetails(new HashMap<String, String[]>());
        }


        public HashMap<String, String[]> getAdditionalDetails() {
            return this.additionalDetails;
        }


        public String getErrorCode() {
            return this.errorCode;
        }


        public String getErrorMessage() {
            return this.errorMessage;
        }


        public void setAdditionalDetails(final HashMap<String, String[]> additionalDetails) {
            this.additionalDetails = additionalDetails;
        }


        public void setErrorCode(final String errorCode) {
            this.errorCode = errorCode;
        }


        public void setErrorMessage(final String errorMessage) {
            this.errorMessage = errorMessage;
        }
    }

    @Deprecated
    public static enum AuthenticationScheme {

        SHAREDKEYLITE,


        SHAREDKEYFULL;
    }

    public abstract static class SharedAccessPolicy {


        private Date sharedAccessExpiryTime;


        private Date sharedAccessStartTime;


        public SharedAccessPolicy() {
            // Empty Default Constructor.
        }


        public Date getSharedAccessExpiryTime() {
            return this.sharedAccessExpiryTime;
        }


        public Date getSharedAccessStartTime() {
            return this.sharedAccessStartTime;
        }


        public void setSharedAccessExpiryTime(final Date sharedAccessExpiryTime) {
            this.sharedAccessExpiryTime = sharedAccessExpiryTime;
        }


        public void setSharedAccessStartTime(final Date sharedAccessStartTime) {
            this.sharedAccessStartTime = sharedAccessStartTime;
        }


        public abstract String permissionsToString();


        public abstract void setPermissionsFromString(final String value);
    }

    public static final class MetricsProperties {

        private String version = "1.0";


        private MetricsLevel metricsLevel = MetricsLevel.DISABLED;


        private Integer retentionIntervalInDays;


        public MetricsLevel getMetricsLevel() {
            return this.metricsLevel;
        }


        public Integer getRetentionIntervalInDays() {
            return this.retentionIntervalInDays;
        }


        public String getVersion() {
            return this.version;
        }


        public void setMetricsLevel(final MetricsLevel metricsLevel) {
            this.metricsLevel = metricsLevel;
        }


        public void setRetentionIntervalInDays(final Integer retentionIntervalInDays) {
            this.retentionIntervalInDays = retentionIntervalInDays;
        }


        public void setVersion(final String version) {
            this.version = version;
        }
    }

    public static class CorsRule {


        private List<String> allowedOrigins = new ArrayList<String>();


        private List<String> exposedHeaders = new ArrayList<String>();


        private List<String> allowedHeaders = new ArrayList<String>();


        private EnumSet<CorsHttpMethods> allowedMethods = EnumSet.noneOf(CorsHttpMethods.class);


        private int maxAgeInSeconds = 0;


        public List<String> getAllowedOrigins() {
            return this.allowedOrigins;
        }


        public void setAllowedOrigins(List<String> allowedOrigins) {
            this.allowedOrigins = allowedOrigins;
        }


        public List<String> getExposedHeaders() {
            return this.exposedHeaders;
        }


        public void setExposedHeaders(List<String> exposedHeaders) {
            this.exposedHeaders = exposedHeaders;
        }


        public List<String> getAllowedHeaders() {
            return this.allowedHeaders;
        }


        public void setAllowedHeaders(List<String> allowedHeaders) {
            this.allowedHeaders = allowedHeaders;
        }


        public EnumSet<CorsHttpMethods> getAllowedMethods() {
            return this.allowedMethods;
        }


        public void setAllowedMethods(EnumSet<CorsHttpMethods> allowedMethods) {
            this.allowedMethods = allowedMethods;
        }


        public int getMaxAgeInSeconds() {
            return this.maxAgeInSeconds;
        }


        public void setMaxAgeInSeconds(int maxAgeInSeconds) {
            this.maxAgeInSeconds = maxAgeInSeconds;
        }

    }

    public static enum StorageLocation {

        PRIMARY,


        SECONDARY;
    }

    public static class ServiceStats {


        private GeoReplicationStats geoReplication;

        protected ServiceStats() {

        }


        public GeoReplicationStats getGeoReplication() {
            return this.geoReplication;
        }


        protected void setGeoReplication(GeoReplicationStats geoReplication) {
            this.geoReplication = geoReplication;
        }
    }

    public static final class RequestCompletedEvent extends BaseEvent {


        public RequestCompletedEvent(OperationContext opContext, Object connectionObject, RequestResult requestResult) {
            super(opContext, connectionObject, requestResult);
        }

    }

    public static final class RetryContext {

        private final StorageLocation nextLocation;


        private final LocationMode locationMode;


        private final int currentRetryCount;


        private final RequestResult lastRequestResult;


        public RetryContext(int currentRetryCount, RequestResult lastRequestResult, StorageLocation nextLocation,
                LocationMode locationMode) {
            this.currentRetryCount = currentRetryCount;
            this.lastRequestResult = lastRequestResult;
            this.nextLocation = nextLocation;
            this.locationMode = locationMode;
        }


        public int getCurrentRetryCount() {
            return this.currentRetryCount;
        }


        public RequestResult getLastRequestResult() {
            return this.lastRequestResult;
        }


        public LocationMode getLocationMode() {
            return this.locationMode;
        }


        public StorageLocation getNextLocation() {
            return this.nextLocation;
        }


        @Override
        public String toString() {
            return String.format(Utility.LOCALE_US, "(%s,%s)", this.currentRetryCount, this.locationMode);
        }

    }

    public static class ResultSegment<T> {

        private final ResultContinuation continuationToken;


        private final int length;


        private final Integer pageSize;


        private final ArrayList<T> results;


        public ResultSegment(final ArrayList<T> results, final Integer pageSize, final ResultContinuation token) {
            this.results = results;
            this.length = results.size();
            this.pageSize = pageSize;
            this.continuationToken = token;
        }


        public ResultContinuation getContinuationToken() {
            return this.continuationToken;
        }


        public boolean getHasMoreResults() {
            return this.continuationToken != null;
        }


        public boolean getIsPageComplete() {
            return (new Integer(this.length)).equals(this.pageSize);
        }


        public int getLength() {
            return this.length;
        }


        public Integer getPageSize() {
            return this.pageSize;
        }


        public int getRemainingPageResults() {
            return this.pageSize - this.length;
        }


        public ArrayList<T> getResults() {
            return this.results;
        }
    }

    public static enum GeoReplicationStatus {


        UNAVAILABLE,


        LIVE,


        BOOTSTRAP;


        protected static GeoReplicationStatus parse(String geoReplicationStatus) {
            if (geoReplicationStatus != null) {
                if (geoReplicationStatus.equals(Constants.GEO_UNAVAILABLE_VALUE)) {
                    return GeoReplicationStatus.UNAVAILABLE;
                }
                else if (geoReplicationStatus.equals(Constants.GEO_LIVE_VALUE)) {
                    return GeoReplicationStatus.LIVE;
                }
                else if (geoReplicationStatus.equals(Constants.GEO_BOOTSTRAP_VALUE)) {
                    return GeoReplicationStatus.BOOTSTRAP;
                }
            }
            throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.INVALID_GEO_REPLICATION_STATUS,
                    geoReplicationStatus));
        }
    }

    public static final class StorageErrorCodeStrings {

        public static final String ACCOUNT_ALREADY_EXISTS = "AccountAlreadyExists";


        public static final String ACCOUNT_BEING_CREATED = "AccountBeingCreated";


        public static final String ACCOUNT_IS_DISABLED = "AccountIsDisabled";


        public static final String AUTHENTICATION_FAILED = "AuthenticationFailed";


        public static final String BLOB_ALREADY_EXISTS = "BlobAlreadyExists";


        public static final String BLOB_NOT_FOUND = "BlobNotFound";


        public static final String CANNOT_VERIFY_COPY_SOURCE = "CannotVerifyCopySource";


        public static final String CANNOT_DELETE_FILE_OR_DIRECTORY = "CannotDeleteFileOrDirectory";


        public static final String CLIENT_CACHE_FLUSH_DELAY = "ClientCacheFlushDelay";


        public static final String CONDITION_HEADERS_NOT_SUPPORTED = "ConditionHeadersNotSupported";


        public static final String CONDITION_NOT_MET = "ConditionNotMet";


        public static final String CONTAINER_ALREADY_EXISTS = "ContainerAlreadyExists";


        public static final String CONTAINER_BEING_DELETED = "ContainerBeingDeleted";


        public static final String CONTAINER_DISABLED = "ContainerDisabled";


        public static final String CONTAINER_NOT_FOUND = "ContainerNotFound";


        public static final String COPY_ACROSS_ACCOUNTS_NOT_SUPPORTED = "CopyAcrossAccountsNotSupported";


        public static final String COPY_ID_MISMATCH = "CopyIdMismatch";


        public static final String DELETE_PENDING = "DeletePending";


        public static final String DIRECTORY_ALREADY_EXISTS = "DirectoryAlreadyExists";


        public static final String DIRECTORY_NOT_EMPTY = "DirectoryNotEmpty";


        public static final String DUPLICATE_PROPERTIES_SPECIFIED = "DuplicatePropertiesSpecified";

        public static final String EMPTY_METADATA_KEY = "EmptyMetadataKey";


        public static final String ENTITY_ALREADY_EXISTS = "EntityAlreadyExists";


        public static final String ENTITY_TOO_LARGE = "EntityTooLarge";


        public static final String FILE_LOCK_CONFLICT = "FileLockConflict";


        public static final String HOST_INFORMATION_NOT_PRESENT = "HostInformationNotPresent";


        public static final String INCORRECT_BLOB_TYPE = "IncorrectBlobType";


        public static final String INFINITE_LEASE_DURATION_REQUIRED = "InfiniteLeaseDurationRequired";


        public static final String INSUFFICIENT_ACCOUNT_PERMISSIONS = "InsufficientAccountPermissions";


        public static final String INTERNAL_ERROR = "InternalError";


        public static final String INVALID_AUTHENTICATION_INFO = "InvalidAuthenticationInfo";


        public static final String INVALID_BLOB_TYPE = "InvalidBlobType";


        public static final String INVALID_BLOB_OR_BLOCK = "InvalidBlobOrBlock";


        public static final String INVALID_BLOCK_ID = "InvalidBlockId";


        public static final String INVALID_BLOCK_LIST = "InvalidBlockList";


        public static final String INVALID_HEADER_VALUE = "InvalidHeaderValue";


        public static final String INVALID_HTTP_VERB = "InvalidHttpVerb";


        public static final String INVALID_INPUT = "InvalidInput";


        public static final String INVALID_MARKER = "InvalidMarker";


        public static final String INVALID_MD5 = "InvalidMd5";


        public static final String INVALID_METADATA = "InvalidMetadata";


        public static final String INVALID_PAGE_RANGE = "InvalidPageRange";


        public static final String INVALID_QUERY_PARAMETER_VALUE = "InvalidQueryParameterValue";


        public static final String INVALID_RANGE = "InvalidRange";


        public static final String INVALID_RESOURCE_NAME = "InvalidResourceName";


        public static final String INVALID_URI = "InvalidUri";


        public static final String INVALID_VALUE_TYPE = "InvalidValueType";


        public static final String INVALID_VERSION_FOR_PAGE_BLOB_OPERATION = "InvalidVersionForPageBlobOperation";


        public static final String INVALID_XML_DOCUMENT = "InvalidXmlDocument";


        public static final String INVALID_XML_NODE_VALUE = "InvalidXmlNodeValue";


        public static final String INVALID_DOCUMENT = "InvalidDocument";


        public static final String INVALID_FILE_OR_DIRECTORY_PATH_NAME = "InvalidFileOrDirectoryPathName";


        public static final String INVALID_TYPE = "InvalidType";


        public static final String JSON_FORMAT_NOT_SUPPORTED = "JsonFormatNotSupported";


        public static final String LEASE_ALREADY_BROKEN = "LeaseAlreadyBroken";


        public static final String LEASE_ALREADY_PRESENT = "LeaseAlreadyPresent";


        public static final String LEASE_ID_MISMATCH_WITH_BLOB_OPERATION = "LeaseIdMismatchWithBlobOperation";


        public static final String LEASE_ID_MISMATCH_WITH_CONTAINER_OPERATION = "LeaseIdMismatchWithContainerOperation";


        public static final String LEASE_ID_MISMATCH_WITH_LEASE_OPERATION = "LeaseIdMismatchWithLeaseOperation";


        public static final String LEASE_ID_MISSING = "LeaseIdMissing";


        public static final String LEASE_IS_BROKEN_AND_CANNOT_BE_RENEWED = "LeaseIsBrokenAndCannotBeRenewed";


        public static final String LEASE_IS_BREAKING_AND_CANNOT_BE_ACQUIRED = "LeaseIsBreakingAndCannotBeAcquired";


        public static final String LEASE_IS_BREAKING_AND_CANNOT_BE_CHANGED = "LeaseIsBreakingAndCannotBeChanged";


        public static final String LEASE_LOST = "LeaseLost";


        public static final String LEASE_NOT_PRESENT_WITH_BLOB_OPERATION = "LeaseNotPresentWithBlobOperation";


        public static final String LEASE_NOT_PRESENT_WITH_CONTAINER_OPERATION = "LeaseNotPresentWithContainerOperation";


        public static final String LEASE_NOT_PRESENT_WITH_LEASE_OPERATION = "LeaseNotPresentWithLeaseOperation";


        public static final String MD5_MISMATCH = "Md5Mismatch";


        public static final String MESSAGE_TOO_LARGE = "MessageTooLarge";


        public static final String MESSAGE_NOT_FOUND = "MessageNotFound";


        public static final String METADATA_TOO_LARGE = "MetadataTooLarge";


        public static final String METHOD_NOT_ALLOWED = "MethodNotAllowed";


        public static final String MISSING_CONTENT_LENGTH_HEADER = "MissingContentLengthHeader";


        public static final String MISSING_REQUIRED_HEADER = "MissingRequiredHeader";


        public static final String MISSING_REQUIRED_QUERY_PARAMETER = "MissingRequiredQueryParameter";


        public static final String MISSING_REQUIRED_XML_NODE = "MissingRequiredXmlNode";


        public static final String MISSING_MD5_HEADER = "MissingContentMD5Header";


        public static final String MULTIPLE_CONDITION_HEADERS_NOT_SUPPORTED = "MultipleConditionHeadersNotSupported";


        public static final String NO_PENDING_COPY_OPERATION = "NoPendingCopyOperation";


        public static final String NOT_IMPLEMENTED = "NotImplemented";


        public static final String OPERATION_TIMED_OUT = "OperationTimedOut";


        public static final String OUT_OF_RANGE_INPUT = "OutOfRangeInput";


        public static final String OUT_OF_RANGE_QUERY_PARAMETER_VALUE = "OutOfRangeQueryParameterValue";


        public static final String PARENT_NOT_FOUND = "ParentNotFound";


        public static final String PENDING_COPY_OPERATION = "PendingCopyOperation";


        public static final String POP_RECEIPT_MISMATCH = "PopReceiptMismatch";


        public static final String PROPERTIES_NEED_VALUE = "PropertiesNeedValue";


        public static final String PROPERTY_NAME_INVALID = "PropertyNameInvalid";


        public static final String PROPERTY_NAME_TOO_LONG = "PropertyNameTooLong";


        public static final String PROPERTY_VALUE_TOO_LARGE = "PropertyValueTooLarge";


        public static final String QUEUE_ALREADY_EXISTS = "QueueAlreadyExists";


        public static final String QUEUE_BEING_DELETED = "QueueBeingDeleted";


        public static final String QUEUE_DISABLED = "QueueDisabled";


        public static final String QUEUE_NOT_EMPTY = "QueueNotEmpty";


        public static final String QUEUE_NOT_FOUND = "QueueNotFound";


        public static final String READ_ONLY_ATTRIBUTE = "ReadOnlyAttribute";


        public static final String REQUEST_BODY_TOO_LARGE = "RequestBodyTooLarge";


        public static final String REQUEST_URL_FAILED_TO_PARSE = "RequestUrlFailedToParse";


        public static final String RESOURCE_NOT_FOUND = "ResourceNotFound";


        public static final String RESOURCE_ALREADY_EXISTS = "ResourceAlreadyExists";


        public static final String RESOURCE_TYPE_MISMATCH = "ResourceTypeMismatch";


        public static final String SEQUENCE_NUMBER_CONDITION_NOT_MET = "SequenceNumberConditionNotMet";


        public static final String SEQUENCE_NUMBER_INCREMENT_TOO_LARGE = "SequenceNumberIncrementTooLarge";


        public static final String SERVER_BUSY = "ServerBusy";


        public static final String SHARE_ALREADY_EXISTS = "ShareAlreadyExists";


        public static final String SHARE_BEING_DELETED = "ShareBeingDeleted";


        public static final String SHARE_DISABLED = "ShareDisabled";


        public static final String SHARE_NOT_FOUND = "ShareNotFound";


        public static final String SHARING_VIOLATION = "SharingViolation";


        public static final String SNAPSHOTS_PRESENT = "SnapshotsPresent";


        public static final String SOURCE_CONDITION_NOT_MET = "SourceConditionNotMet";


        public static final String TARGET_CONDITION_NOT_MET = "TargetConditionNotMet";


        public static final String TABLE_ALREADY_EXISTS = "TableAlreadyExists";


        public static final String TABLE_BEING_DELETED = "TableBeingDeleted";


        public static final String TABLE_NOT_FOUND = "TableNotFound";


        public static final String TOO_MANY_PROPERTIES = "TooManyProperties";


        public static final String UPDATE_CONDITION_NOT_SATISFIED = "UpdateConditionNotSatisfied";


        public static final String UNSUPPORTED_HEADER = "UnsupportedHeader";


        public static final String UNSUPPORTED_XML_NODE = "UnsupportedXmlNode";


        public static final String UNSUPPORTED_HTTP_VERB = "UnsupportedHttpVerb";


        public static final String UNSUPPORTED_QUERY_PARAMETER = "UnsupportedQueryParameter";


        public static final String X_METHOD_INCORRECT_COUNT = "XMethodIncorrectCount";


        public static final String X_METHOD_INCORRECT_VALUE = "XMethodIncorrectValue";


        public static final String X_METHOD_NOT_USING_POST = "XMethodNotUsingPost";


        private StorageErrorCodeStrings() {
            // No op
        }
    }

    public static final class SendingRequestEvent extends BaseEvent {


        public SendingRequestEvent(OperationContext opContext, Object connectionObject, RequestResult requestResult) {
            super(opContext, connectionObject, requestResult);
        }

    }

    public static final class AccessCondition {

        public static AccessCondition generateEmptyCondition() {
            return new AccessCondition();
        }


        public static AccessCondition generateIfMatchCondition(final String etag) {
            AccessCondition retCondition = new AccessCondition();
            retCondition.setIfMatch(etag);
            return retCondition;
        }


        public static AccessCondition generateIfModifiedSinceCondition(final Date lastMotified) {
            AccessCondition retCondition = new AccessCondition();
            retCondition.ifModifiedSinceDate = lastMotified;
            return retCondition;
        }


        public static AccessCondition generateIfNoneMatchCondition(final String etag) {
            AccessCondition retCondition = new AccessCondition();
            retCondition.setIfNoneMatch(etag);
            return retCondition;
        }


        public static AccessCondition generateIfNotModifiedSinceCondition(final Date lastMotified) {
            AccessCondition retCondition = new AccessCondition();
            retCondition.ifUnmodifiedSinceDate = lastMotified;
            return retCondition;
        }


        public static AccessCondition generateIfSequenceNumberLessThanOrEqualCondition(long sequenceNumber) {
            AccessCondition retCondition = new AccessCondition();
            retCondition.ifSequenceNumberLessThanOrEqual = sequenceNumber;
            return retCondition;
        }


        public static AccessCondition generateIfSequenceNumberLessThanCondition(long sequenceNumber) {
            AccessCondition retCondition = new AccessCondition();
            retCondition.ifSequenceNumberLessThan = sequenceNumber;
            return retCondition;
        }


        public static AccessCondition generateIfSequenceNumberEqualCondition(long sequenceNumber) {
            AccessCondition retCondition = new AccessCondition();
            retCondition.ifSequenceNumberEqual = sequenceNumber;
            return retCondition;
        }


        public static AccessCondition generateLeaseCondition(final String leaseID) {
            AccessCondition retCondition = new AccessCondition();
            retCondition.leaseID = leaseID;
            return retCondition;
        }

        private String leaseID = null;


        private String ifMatchETag = null;


        private String ifNoneMatchETag = null;


        private Date ifModifiedSinceDate = null;


        private Date ifUnmodifiedSinceDate = null;


        private Long ifSequenceNumberLessThanOrEqual = null;


        private Long ifSequenceNumberLessThan = null;


        private Long ifSequenceNumberEqual = null;


        public AccessCondition() {
            // Empty default constructor.
        }


        public void applyConditionToRequest(final HttpURLConnection request) {
            applyLeaseConditionToRequest(request);

            if (this.ifModifiedSinceDate != null) {
                request.setRequestProperty(Constants.HeaderConstants.IF_MODIFIED_SINCE,
                        Utility.getGMTTime(this.ifModifiedSinceDate));
            }

            if (this.ifUnmodifiedSinceDate != null) {
                request.setRequestProperty(Constants.HeaderConstants.IF_UNMODIFIED_SINCE,
                        Utility.getGMTTime(this.ifUnmodifiedSinceDate));
            }

            if (!Utility.isNullOrEmpty(this.ifMatchETag)) {
                request.setRequestProperty(Constants.HeaderConstants.IF_MATCH, this.ifMatchETag);
            }

            if (!Utility.isNullOrEmpty(this.ifNoneMatchETag)) {
                request.setRequestProperty(Constants.HeaderConstants.IF_NONE_MATCH, this.ifNoneMatchETag);
            }
        }


        public void applySourceConditionToRequest(final HttpURLConnection request) {
            if (!Utility.isNullOrEmpty(this.leaseID)) {
                // Unsupported
                throw new IllegalArgumentException(SR.LEASE_CONDITION_ON_SOURCE);
            }

            if (this.ifModifiedSinceDate != null) {
                request.setRequestProperty(
                        Constants.HeaderConstants.SOURCE_IF_MODIFIED_SINCE_HEADER,
                        Utility.getGMTTime(this.ifModifiedSinceDate));
            }

            if (this.ifUnmodifiedSinceDate != null) {
                request.setRequestProperty(Constants.HeaderConstants.SOURCE_IF_UNMODIFIED_SINCE_HEADER,
                        Utility.getGMTTime(this.ifUnmodifiedSinceDate));
            }

            if (!Utility.isNullOrEmpty(this.ifMatchETag)) {
                request.setRequestProperty(
                        Constants.HeaderConstants.SOURCE_IF_MATCH_HEADER,
                        this.ifMatchETag);
            }

            if (!Utility.isNullOrEmpty(this.ifNoneMatchETag)) {
                request.setRequestProperty(
                        Constants.HeaderConstants.SOURCE_IF_NONE_MATCH_HEADER,
                        this.ifNoneMatchETag);
            }
        }


        public void applyLeaseConditionToRequest(final HttpURLConnection request) {
            if (!Utility.isNullOrEmpty(this.leaseID)) {
                request.setRequestProperty(Constants.HeaderConstants.LEASE_ID_HEADER, this.leaseID);
            }
        }


        public void applySequenceConditionToRequest(final HttpURLConnection request) {
            if (this.ifSequenceNumberLessThanOrEqual != null) {
                request.setRequestProperty(
                        Constants.HeaderConstants.IF_SEQUENCE_NUMBER_LESS_THAN_OR_EQUAL,
                        this.ifSequenceNumberLessThanOrEqual.toString());
            }

            if (this.ifSequenceNumberLessThan != null) {
                request.setRequestProperty(
                        Constants.HeaderConstants.IF_SEQUENCE_NUMBER_LESS_THAN,
                        this.ifSequenceNumberLessThan.toString());
            }

            if (this.ifSequenceNumberEqual != null) {
                request.setRequestProperty(
                        Constants.HeaderConstants.IF_SEQUENCE_NUMBER_EQUAL,
                        this.ifSequenceNumberEqual.toString());
            }
        }


        public String getIfMatch() {
            return this.ifMatchETag;
        }


        public Date getIfModifiedSinceDate() {
            return this.ifModifiedSinceDate;
        }


        public String getIfNoneMatch() {
            return this.ifNoneMatchETag;
        }


        public Date getIfUnmodifiedSinceDate() {
            return this.ifUnmodifiedSinceDate;
        }


        public String getLeaseID() {
            return this.leaseID;
        }


        public Long getIfSequenceNumberLessThanOrEqual() {
            return this.ifSequenceNumberLessThanOrEqual;
        }


        public Long getIfSequenceNumberLessThan() {
            return this.ifSequenceNumberLessThan;
        }


        public Long getIfSequenceNumberEqual() {
            return this.ifSequenceNumberEqual;
        }


        public void setIfMatch(String etag) {
            this.ifMatchETag = normalizeEtag(etag);
        }


        public void setIfModifiedSinceDate(Date ifModifiedSinceDate) {
            this.ifModifiedSinceDate = ifModifiedSinceDate;
        }


        public void setIfNoneMatch(String etag) {
            this.ifNoneMatchETag = normalizeEtag(etag);
        }


        public void setIfUnmodifiedSinceDate(Date ifUnmodifiedSinceDate) {
            this.ifUnmodifiedSinceDate = ifUnmodifiedSinceDate;
        }


        public void setLeaseID(String leaseID) {
            this.leaseID = leaseID;
        }


        public void setIfSequenceNumberLessThanOrEqual(Long sequenceNumber) {
            this.ifSequenceNumberLessThanOrEqual = sequenceNumber;
        }


        public void setIfSequenceNumberLessThan(Long sequenceNumber) {
            this.ifSequenceNumberLessThan = sequenceNumber;
        }


        public void setIfSequenceNumberEqual(Long sequenceNumber) {
            this.ifSequenceNumberEqual = sequenceNumber;
        }


        public boolean verifyConditional(final String etag, final Date lastModified) {
            if (this.ifModifiedSinceDate != null) {
                // The IfModifiedSince has a special helper in HttpURLConnection, use it instead of manually setting the
                // header.
                if (!lastModified.after(this.ifModifiedSinceDate)) {
                    return false;
                }
            }

            if (this.ifUnmodifiedSinceDate != null) {
                if (lastModified.after(this.ifUnmodifiedSinceDate)) {
                    return false;
                }
            }

            if (!Utility.isNullOrEmpty(this.ifMatchETag)) {
                if (!this.ifMatchETag.equals(etag) && !this.ifMatchETag.equals("*")) {
                    return false;
                }
            }

            if (!Utility.isNullOrEmpty(this.ifNoneMatchETag)) {
                if (this.ifNoneMatchETag.equals(etag)) {
                    return false;
                }
            }

            return true;
        }


        private static String normalizeEtag(String inTag) {
            if (Utility.isNullOrEmpty(inTag) || inTag.equals("*")) {
                return inTag;
            }
            else if (inTag.startsWith("\"") && inTag.endsWith("\"")) {
                return inTag;
            }
            else {
                return String.format("\"%s\"", inTag);
            }
        }
    }

    static final class ServicePropertiesSerializer {


        public static byte[] serializeToByteArray(final ServiceProperties properties) throws XMLStreamException,
                StorageException {
            final StringWriter outWriter = new StringWriter();
            final XMLStreamWriter xmlw = Utility.createXMLStreamWriter(outWriter);

            // default is UTF8
            xmlw.writeStartDocument();
            xmlw.writeStartElement(Constants.AnalyticsConstants.STORAGE_SERVICE_PROPERTIES_ELEMENT);

            if (properties.getLogging() != null) {
                // Logging Properties
                writeLoggingProperties(xmlw, properties.getLogging());
            }

            if (properties.getHourMetrics() != null) {
                // Hour Metrics
                writeMetricsProperties(xmlw, properties.getHourMetrics(), Constants.AnalyticsConstants.HOUR_METRICS_ELEMENT);
            }

            if (properties.getMinuteMetrics() != null) {
                // Minute Metrics
                writeMetricsProperties(xmlw, properties.getMinuteMetrics(),
                        Constants.AnalyticsConstants.MINUTE_METRICS_ELEMENT);
            }

            if (properties.getCors() != null) {
                // CORS Properties
                writeCorsProperties(xmlw, properties.getCors());
            }

            // Default Service Version
            if (properties.getDefaultServiceVersion() != null) {
                xmlw.writeStartElement(Constants.AnalyticsConstants.DEFAULT_SERVICE_VERSION);
                xmlw.writeCharacters(properties.getDefaultServiceVersion());
                xmlw.writeEndElement();
            }

            // end StorageServiceProperties
            xmlw.writeEndElement();

            // end doc
            xmlw.writeEndDocument();

            try {
                return outWriter.toString().getBytes(Constants.UTF8_CHARSET);
            }
            catch (final UnsupportedEncodingException e) {
                throw Utility.generateNewUnexpectedStorageException(e);
            }
        }


        private static void writeRetentionPolicy(final XMLStreamWriter xmlw, final Integer val) throws XMLStreamException {
            xmlw.writeStartElement(Constants.AnalyticsConstants.RETENTION_POLICY_ELEMENT);

            // Enabled
            xmlw.writeStartElement(Constants.AnalyticsConstants.ENABLED_ELEMENT);
            xmlw.writeCharacters(val != null ? Constants.TRUE : Constants.FALSE);
            xmlw.writeEndElement();

            if (val != null) {
                // Days
                xmlw.writeStartElement(Constants.AnalyticsConstants.DAYS_ELEMENT);
                xmlw.writeCharacters(val.toString());
                xmlw.writeEndElement();
            }

            // End Retention Policy
            xmlw.writeEndElement();
        }


        private static void writeCorsProperties(final XMLStreamWriter xmlw, final CorsProperties cors)
                throws XMLStreamException {
            Utility.assertNotNull("CorsRules", cors.getCorsRules());

            // CORS
            xmlw.writeStartElement(Constants.AnalyticsConstants.CORS_ELEMENT);

            for (CorsRule rule : cors.getCorsRules()) {
                if (rule.getAllowedOrigins().isEmpty() || rule.getAllowedMethods().isEmpty()
                        || rule.getMaxAgeInSeconds() < 0) {
                    throw new IllegalArgumentException(SR.INVALID_CORS_RULE);
                }

                xmlw.writeStartElement(Constants.AnalyticsConstants.CORS_RULE_ELEMENT);

                xmlw.writeStartElement(Constants.AnalyticsConstants.ALLOWED_ORIGINS_ELEMENT);
                xmlw.writeCharacters(joinToString(rule.getAllowedOrigins(), ","));
                xmlw.writeEndElement();

                xmlw.writeStartElement(Constants.AnalyticsConstants.ALLOWED_METHODS_ELEMENT);
                xmlw.writeCharacters(joinToString(rule.getAllowedMethods(), ","));
                xmlw.writeEndElement();

                xmlw.writeStartElement(Constants.AnalyticsConstants.EXPOSED_HEADERS_ELEMENT);
                xmlw.writeCharacters(joinToString(rule.getExposedHeaders(), ","));
                xmlw.writeEndElement();

                xmlw.writeStartElement(Constants.AnalyticsConstants.ALLOWED_HEADERS_ELEMENT);
                xmlw.writeCharacters(joinToString(rule.getAllowedHeaders(), ","));
                xmlw.writeEndElement();

                xmlw.writeStartElement(Constants.AnalyticsConstants.MAX_AGE_IN_SECONDS_ELEMENT);
                xmlw.writeCharacters(Integer.toString(rule.getMaxAgeInSeconds()));
                xmlw.writeEndElement();

                xmlw.writeEndElement();
            }

            // end CORS
            xmlw.writeEndElement();
        }


        private static void writeMetricsProperties(final XMLStreamWriter xmlw, final MetricsProperties metrics,
                final String metricsName) throws XMLStreamException {
            Utility.assertNotNull("metrics.Configuration", metrics.getMetricsLevel());

            // Metrics
            xmlw.writeStartElement(metricsName);

            // Version
            xmlw.writeStartElement(Constants.AnalyticsConstants.VERSION_ELEMENT);
            xmlw.writeCharacters(metrics.getVersion());
            xmlw.writeEndElement();

            // Enabled
            xmlw.writeStartElement(Constants.AnalyticsConstants.ENABLED_ELEMENT);
            xmlw.writeCharacters(metrics.getMetricsLevel() != MetricsLevel.DISABLED ? Constants.TRUE : Constants.FALSE);
            xmlw.writeEndElement();

            if (metrics.getMetricsLevel() != MetricsLevel.DISABLED) {
                // Include APIs
                xmlw.writeStartElement(Constants.AnalyticsConstants.INCLUDE_APIS_ELEMENT);
                xmlw.writeCharacters(metrics.getMetricsLevel() == MetricsLevel.SERVICE_AND_API ? Constants.TRUE
                        : Constants.FALSE);
                xmlw.writeEndElement();
            }

            // Retention Policy
            writeRetentionPolicy(xmlw, metrics.getRetentionIntervalInDays());

            // end Metrics
            xmlw.writeEndElement();
        }


        private static void writeLoggingProperties(final XMLStreamWriter xmlw, final LoggingProperties logging)
                throws XMLStreamException {
            Utility.assertNotNull("logging.LogOperationTypes", logging.getLogOperationTypes());

            // Logging
            xmlw.writeStartElement(Constants.AnalyticsConstants.LOGGING_ELEMENT);

            // Version
            xmlw.writeStartElement(Constants.AnalyticsConstants.VERSION_ELEMENT);
            xmlw.writeCharacters(logging.getVersion());
            xmlw.writeEndElement();

            // Delete
            xmlw.writeStartElement(Constants.AnalyticsConstants.DELETE_ELEMENT);
            xmlw.writeCharacters(logging.getLogOperationTypes().contains(LoggingOperations.DELETE) ? Constants.TRUE
                    : Constants.FALSE);
            xmlw.writeEndElement();

            // Read
            xmlw.writeStartElement(Constants.AnalyticsConstants.READ_ELEMENT);
            xmlw.writeCharacters(logging.getLogOperationTypes().contains(LoggingOperations.READ) ? Constants.TRUE
                    : Constants.FALSE);
            xmlw.writeEndElement();

            // Write
            xmlw.writeStartElement(Constants.AnalyticsConstants.WRITE_ELEMENT);
            xmlw.writeCharacters(logging.getLogOperationTypes().contains(LoggingOperations.WRITE) ? Constants.TRUE
                    : Constants.FALSE);
            xmlw.writeEndElement();

            // Retention Policy
            writeRetentionPolicy(xmlw, logging.getRetentionIntervalInDays());

            // end Logging
            xmlw.writeEndElement();
        }


        private static String joinToString(Iterable<?> iterable, String delimiter) {
            StringBuilder builder = new StringBuilder();
            Iterator<?> iter = iterable.iterator();
            while (iter.hasNext()) {
                builder.append(iter.next());
                if (iter.hasNext()) {
                    builder.append(delimiter);
                }
            }
            return builder.toString();
        }
    }

    public abstract static class RequestOptions {


        private RetryPolicyFactory retryPolicyFactory;


        private Integer timeoutIntervalInMs;


        private LocationMode locationMode;


        private Integer maximumExecutionTimeInMs;


        private Long operationExpiryTime;


        public RequestOptions() {
            // Empty Default Ctor
        }


        public RequestOptions(final RequestOptions other) {
            if (other != null) {
                this.setRetryPolicyFactory(other.getRetryPolicyFactory());
                this.setTimeoutIntervalInMs(other.getTimeoutIntervalInMs());
                this.setLocationMode(other.getLocationMode());
                this.setMaximumExecutionTimeInMs(other.getMaximumExecutionTimeInMs());
                this.setOperationExpiryTimeInMs(other.getOperationExpiryTimeInMs());
            }
        }


        protected static final RequestOptions applyBaseDefaultsInternal(final RequestOptions modifiedOptions) {
            Utility.assertNotNull("modifiedOptions", modifiedOptions);
            if (modifiedOptions.getRetryPolicyFactory() == null) {
                modifiedOptions.setRetryPolicyFactory(new RetryExponentialRetry());
            }

            if (modifiedOptions.getLocationMode() == null) {
                modifiedOptions.setLocationMode(LocationMode.PRIMARY_ONLY);
            }

            return modifiedOptions;
        }


        protected static final RequestOptions populateRequestOptions(RequestOptions modifiedOptions,
                final RequestOptions clientOptions, final boolean setStartTime) {
            if (modifiedOptions.getRetryPolicyFactory() == null) {
                modifiedOptions.setRetryPolicyFactory(clientOptions.getRetryPolicyFactory());
            }

            if (modifiedOptions.getLocationMode() == null) {
                modifiedOptions.setLocationMode(clientOptions.getLocationMode());
            }

            if (modifiedOptions.getTimeoutIntervalInMs() == null) {
                modifiedOptions.setTimeoutIntervalInMs(clientOptions.getTimeoutIntervalInMs());
            }

            if (modifiedOptions.getMaximumExecutionTimeInMs() == null) {
                modifiedOptions.setMaximumExecutionTimeInMs(clientOptions.getMaximumExecutionTimeInMs());
            }

            if (modifiedOptions.getMaximumExecutionTimeInMs() != null
                    && modifiedOptions.getOperationExpiryTimeInMs() == null && setStartTime) {
                modifiedOptions.setOperationExpiryTimeInMs(new Date().getTime()
                        + modifiedOptions.getMaximumExecutionTimeInMs());
            }

            return modifiedOptions;
        }


        public final RetryPolicyFactory getRetryPolicyFactory() {
            return this.retryPolicyFactory;
        }


        public final Integer getTimeoutIntervalInMs() {
            return this.timeoutIntervalInMs;
        }


        public final LocationMode getLocationMode() {
            return this.locationMode;
        }


        public Integer getMaximumExecutionTimeInMs() {
            return this.maximumExecutionTimeInMs;
        }


        public Long getOperationExpiryTimeInMs() {
            return this.operationExpiryTime;
        }


        public final void setRetryPolicyFactory(final RetryPolicyFactory retryPolicyFactory) {
            this.retryPolicyFactory = retryPolicyFactory;
        }


        public final void setTimeoutIntervalInMs(final Integer timeoutIntervalInMs) {
            this.timeoutIntervalInMs = timeoutIntervalInMs;
        }


        public void setLocationMode(final LocationMode locationMode) {
            this.locationMode = locationMode;
        }


        public void setMaximumExecutionTimeInMs(Integer maximumExecutionTimeInMs) {
            this.maximumExecutionTimeInMs = maximumExecutionTimeInMs;
        }


        private void setOperationExpiryTimeInMs(final Long operationExpiryTime) {
            this.operationExpiryTime = operationExpiryTime;
        }
    }

    public static final class ServiceProperties {


        private LoggingProperties logging;


        private MetricsProperties hourMetrics;


        private MetricsProperties minuteMetrics;


        private CorsProperties cors;


        private String defaultServiceVersion;


        public ServiceProperties() {
            this.setLogging(new LoggingProperties());
            this.setHourMetrics(new MetricsProperties());
            this.setMinuteMetrics(new MetricsProperties());
            this.setCors(new CorsProperties());
        }


        public LoggingProperties getLogging() {
            return this.logging;
        }


        public void setLogging(final LoggingProperties logging) {
            this.logging = logging;
        }


        public MetricsProperties getHourMetrics() {
            return this.hourMetrics;
        }


        public void setHourMetrics(final MetricsProperties metrics) {
            this.hourMetrics = metrics;
        }


        public MetricsProperties getMinuteMetrics() {
            return this.minuteMetrics;
        }


        public void setMinuteMetrics(final MetricsProperties metrics) {
            this.minuteMetrics = metrics;
        }


        public CorsProperties getCors() {
            return this.cors;
        }


        public void setCors(final CorsProperties cors) {
            this.cors = cors;
        }


        public String getDefaultServiceVersion() {
            return this.defaultServiceVersion;
        }


        public void setDefaultServiceVersion(final String defaultServiceVersion) {
            this.defaultServiceVersion = defaultServiceVersion;
        }
    }

    public static final class RetryNoRetry extends RetryPolicy implements RetryPolicyFactory {


        private static RetryNoRetry instance = new RetryNoRetry();


        public static RetryNoRetry getInstance() {
            return instance;
        }


        @Override
        public RetryPolicy createInstance(final OperationContext opContext) {
            return getInstance();
        }


        @Override
        public RetryInfo evaluate(RetryContext retryContext, OperationContext operationContext) {
            return null;
        }
    }

    public abstract static class StorageEvent<T extends BaseEvent> implements EventListener {

        public abstract void eventOccurred(T eventArg);
    }

    public static class SharedAccessPolicyHandler<T extends SharedAccessPolicy> extends DefaultHandler {

        private final Stack<String> elementStack = new Stack<String>();
        private StringBuilder bld = new StringBuilder();

        private final HashMap<String, T> policies = new HashMap<String, T>();
        private final Class<T> policyClassType;

        private String id;
        private T policy;

        private SharedAccessPolicyHandler(final Class<T> cls) {
            this.policyClassType = cls;
        }


        public static <T extends SharedAccessPolicy> HashMap<String, T> getAccessIdentifiers(final InputStream stream,
                final Class<T> cls) throws ParserConfigurationException, SAXException, IOException {
            SAXParser saxParser = Utility.getSAXParser();
            SharedAccessPolicyHandler<T> handler = new SharedAccessPolicyHandler<T>(cls);
            saxParser.parse(stream, handler);

            return handler.policies;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            this.elementStack.push(localName);

            if (Constants.SIGNED_IDENTIFIER_ELEMENT.equals(localName)) {
                this.id = null;

                try {
                    this.policy = this.policyClassType.newInstance();
                }
                catch (Exception e) {
                    throw new SAXException(e);
                }
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            String currentNode = this.elementStack.pop();

            // if the node popped from the stack and the localName don't match, the xml document is improperly formatted
            if (!localName.equals(currentNode)) {
                throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
            }

            String value = this.bld.toString();
            if (value.isEmpty()) {
                value = null;
            }

            if (Constants.SIGNED_IDENTIFIER_ELEMENT.equals(currentNode)) {
                this.policies.put(this.id, this.policy);
            }
            else if (Constants.ID.equals(currentNode)) {
                this.id = value;
            }
            else if (Constants.START.equals(currentNode)) {
                try {
                    this.policy.setSharedAccessStartTime(Utility.parseDate(value));
                }
                catch (IllegalArgumentException e) {
                    throw new SAXException(e);
                }
            }
            else if (Constants.EXPIRY.equals(currentNode)) {
                try {
                    this.policy.setSharedAccessExpiryTime(Utility.parseDate(value));
                }
                catch (IllegalArgumentException e) {
                    throw new SAXException(e);
                }
            }
            else if (Constants.PERMISSION.equals(currentNode)) {
                this.policy.setPermissionsFromString(value);
            }

            this.bld = new StringBuilder();
        }

        @Override
        public void characters(char ch[], int start, int length) throws SAXException {
            this.bld.append(ch, start, length);
        }
    }

    public abstract static class StorageCredentials {


        protected static StorageCredentials tryParseCredentials(final Map<String, String> settings)
                throws InvalidKeyException {
            final String accountName = settings.get(CloudStorageAccount.ACCOUNT_NAME_NAME) != null ? settings
                    .get(CloudStorageAccount.ACCOUNT_NAME_NAME) : null;

            final String accountKey = settings.get(CloudStorageAccount.ACCOUNT_KEY_NAME) != null ? settings
                    .get(CloudStorageAccount.ACCOUNT_KEY_NAME) : null;

            final String sasSignature = settings.get(CloudStorageAccount.SHARED_ACCESS_SIGNATURE_NAME) != null ? settings
                    .get(CloudStorageAccount.SHARED_ACCESS_SIGNATURE_NAME) : null;

            if (accountName != null && accountKey != null && sasSignature == null) {
                if (Base64.validateIsBase64String(accountKey)) {
                    return new StorageCredentialsAccountAndKey(accountName, accountKey);
                }
                else {
                    throw new InvalidKeyException(SR.INVALID_KEY);
                }
            }
            if (accountName == null && accountKey == null && sasSignature != null) {
                return new StorageCredentialsSharedAccessSignature(sasSignature);
            }

            return null;
        }


        public static StorageCredentials tryParseCredentials(final String connectionString) throws InvalidKeyException {
            return tryParseCredentials(Utility.parseAccountString(connectionString));
        }


        public String getAccountName() {
            return null;
        }


        public abstract String toString(boolean exportSecrets);


        public URI transformUri(final URI resourceUri) throws URISyntaxException, StorageException {
            return this.transformUri(resourceUri, null);
        }


        public StorageUri transformUri(StorageUri resourceUri) throws URISyntaxException, StorageException {
            return this.transformUri(resourceUri, null );
        }


        public abstract URI transformUri(URI resourceUri, OperationContext opContext) throws URISyntaxException,
                StorageException;


        public abstract StorageUri transformUri(StorageUri resourceUri, OperationContext opContext)
                throws URISyntaxException, StorageException;
    }

    public abstract static class RetryPolicy implements RetryPolicyFactory {


        public static final int DEFAULT_CLIENT_BACKOFF = 1000 * 30;


        public static final int DEFAULT_CLIENT_RETRY_COUNT = 3;


        public static final int DEFAULT_MAX_BACKOFF = 1000 * 90;


        public static final int DEFAULT_MIN_BACKOFF = 1000 * 3;


        protected int deltaBackoffIntervalInMs;


        protected int maximumAttempts;


        protected Date lastPrimaryAttempt = null;


        protected Date lastSecondaryAttempt = null;


        public RetryPolicy() {
            // Empty Default Ctor
        }


        public RetryPolicy(final int deltaBackoff, final int maxAttempts) {
            this.deltaBackoffIntervalInMs = deltaBackoff;
            this.maximumAttempts = maxAttempts;
        }


        public abstract RetryInfo evaluate(RetryContext retryContext, OperationContext operationContext);


        protected boolean evaluateLastAttemptAndSecondaryNotFound(RetryContext retryContext) {
            Utility.assertNotNull("retryContext", retryContext);

            // Retry interval of a request to a location must take the time spent sending requests
            // to other locations into account. For example, assume a request was sent to the primary
            // location first, then to the secondary, and then to the primary again. If it
            // was supposed to wait 10 seconds between requests to the primary and the request to
            // the secondary took 3 seconds in total, retry interval should only be 7 seconds. This is because,
            // in total, the requests will be 10 seconds apart from the primary locations' point of view.
            // For this calculation, current instance of the retry policy stores the time of the last
            // request to a specific location.
            if (retryContext.getLastRequestResult().getTargetLocation() == StorageLocation.PRIMARY) {
                this.lastPrimaryAttempt = retryContext.getLastRequestResult().getStopDate();
            }
            else {
                this.lastSecondaryAttempt = retryContext.getLastRequestResult().getStopDate();
            }

            // If a request sent to the secondary location fails with 404 (Not Found), it is possible
            // that the the asynchronous geo-replication for the resource has not completed. So, in case of 404 only in the secondary
            // location, the failure should still be retried.
            return (retryContext.getLastRequestResult().getTargetLocation() == StorageLocation.SECONDARY)
                    && (retryContext.getLastRequestResult().getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND);
        }


        protected RetryInfo evaluateRetryInfo(final RetryContext retryContext, final boolean secondaryNotFound,
                final long retryInterval) {
            RetryInfo retryInfo = new RetryInfo(retryContext);

            // Moreover, in case of 404 when trying the secondary location, instead of retrying on the
            // secondary, further requests should be sent only to the primary location, as it most
            // probably has a higher chance of succeeding there.
            if (secondaryNotFound && (retryContext.getLocationMode() != LocationMode.SECONDARY_ONLY)) {
                retryInfo.setUpdatedLocationMode(LocationMode.PRIMARY_ONLY);
                retryInfo.setTargetLocation(StorageLocation.PRIMARY);
            }

            // Now is the time to calculate the exact retry interval. ShouldRetry call above already
            // returned back how long two requests to the same location should be apart from each other.
            // However, for the reasons explained above, the time spent between the last attempt to
            // the target location and current time must be subtracted from the total retry interval
            // that ShouldRetry returned.
            Date lastAttemptTime = retryInfo.getTargetLocation() == StorageLocation.PRIMARY ? this.lastPrimaryAttempt
                    : this.lastSecondaryAttempt;
            if (lastAttemptTime != null) {
                long sinceLastAttempt = (new Date().getTime() - lastAttemptTime.getTime() > 0) ? new Date().getTime()
                        - lastAttemptTime.getTime() : 0;
                retryInfo.setRetryInterval((int) (retryInterval - sinceLastAttempt));
            }
            else {
                retryInfo.setRetryInterval(0);
            }

            return retryInfo;
        }
    }

    public static enum CorsHttpMethods {
        GET, HEAD, POST, PUT, DELETE, TRACE, OPTIONS, CONNECT, MERGE
    }

    static final class ServiceStatsHandler extends DefaultHandler {

        private final static String GEO_REPLICATION_NAME = "GeoReplication";


        private final static String STATUS_NAME = "Status";


        private final static String LAST_SYNC_TIME_NAME = "LastSyncTime";

        private final Stack<String> elementStack = new Stack<String>();
        private StringBuilder bld = new StringBuilder();

        private final GeoReplicationStats geoReplicationStats = new GeoReplicationStats();
        private final ServiceStats stats = new ServiceStats();


        public static ServiceStats readServiceStatsFromStream(final InputStream inStream)
                throws ParserConfigurationException, SAXException, IOException {
            SAXParser saxParser = Utility.getSAXParser();
            ServiceStatsHandler handler = new ServiceStatsHandler();
            saxParser.parse(inStream, handler);

            return handler.stats;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            this.elementStack.push(localName);
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            String currentNode = this.elementStack.pop();

            // if the node popped from the stack and the localName don't match, the xml document is improperly formatted
            if (!localName.equals(currentNode)) {
                throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
            }

            String value = this.bld.toString();
            if (value.isEmpty()) {
                value = null;
            }

            if (GEO_REPLICATION_NAME.equals(currentNode)) {
                this.stats.setGeoReplication(this.geoReplicationStats);
            }
            else if (STATUS_NAME.equals(currentNode)) {
                this.geoReplicationStats.setStatus(GeoReplicationStatus.parse(value));
            }
            else if (LAST_SYNC_TIME_NAME.equals(currentNode)) {
                try {
                    this.geoReplicationStats.setLastSyncTime(Utility.isNullOrEmpty(value) ? null : Utility
                            .parseRFC1123DateFromStringInGMT(value));
                }
                catch (ParseException e) {
                    throw new SAXException(e);
                }
            }

            this.bld = new StringBuilder();
        }

        @Override
        public void characters(char ch[], int start, int length) throws SAXException {
            this.bld.append(ch, start, length);
        }

    }

    public static final class ResponseReceivedEvent extends BaseEvent {


        public ResponseReceivedEvent(OperationContext opContext, Object connectionObject, RequestResult requestResult) {
            super(opContext, connectionObject, requestResult);
        }

    }

    public static final class CloudStorageAccount {

        protected static final String ACCOUNT_KEY_NAME = "AccountKey";


        protected static final String ACCOUNT_NAME_NAME = "AccountName";


        private static final String DNS_NAME_FORMAT = "%s.%s";


        private static final String DEFAULT_DNS = "core.windows.net";


        private static final String SECONDARY_LOCATION_ACCOUNT_SUFFIX = "-secondary";


        private static final String ENDPOINT_SUFFIX_NAME = "EndpointSuffix";


        protected static final String BLOB_ENDPOINT_NAME = "BlobEndpoint";


        private static final String DEFAULT_ENDPOINTS_PROTOCOL_NAME = "DefaultEndpointsProtocol";


        private static final String DEVELOPMENT_STORAGE_PRIMARY_ENDPOINT_FORMAT = "%s://%s:%s/%s";


        private static final String DEVELOPMENT_STORAGE_SECONDARY_ENDPOINT_FORMAT =
                DEVELOPMENT_STORAGE_PRIMARY_ENDPOINT_FORMAT + SECONDARY_LOCATION_ACCOUNT_SUFFIX;


        private static final String DEVELOPMENT_STORAGE_PROXY_URI_NAME = "DevelopmentStorageProxyUri";


        private static final String DEVSTORE_ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";


        private static final String DEVSTORE_ACCOUNT_NAME = "devstoreaccount1";


        private static final String FILE_ENDPOINT_NAME = "FileEndpoint";


        private static final String PRIMARY_ENDPOINT_FORMAT = "%s://%s.%s";


        private static final String SECONDARY_ENDPOINT_FORMAT = "%s://%s%s.%s";


        protected static final String QUEUE_ENDPOINT_NAME = "QueueEndpoint";


        protected static final String SHARED_ACCESS_SIGNATURE_NAME = "SharedAccessSignature";


        protected static final String TABLE_ENDPOINT_NAME = "TableEndpoint";


        private static final String USE_DEVELOPMENT_STORAGE_NAME = "UseDevelopmentStorage";


        public static CloudStorageAccount getDevelopmentStorageAccount() {
            try {
                return getDevelopmentStorageAccount(null);
            }
            catch (final URISyntaxException e) {
                // this won't happen since we know the standard development stororage uri is valid.
                return null;
            }
        }


        public static CloudStorageAccount getDevelopmentStorageAccount(final URI proxyUri) throws URISyntaxException {
            String scheme;
            String host;
            if (proxyUri == null) {
                scheme = "http";
                host = "127.0.0.1";
            }
            else {
                scheme = proxyUri.getScheme();
                host = proxyUri.getHost();
            }

            StorageCredentials credentials = new StorageCredentialsAccountAndKey(DEVSTORE_ACCOUNT_NAME,
                    DEVSTORE_ACCOUNT_KEY);

            URI blobPrimaryEndpoint = new URI(String.format(DEVELOPMENT_STORAGE_PRIMARY_ENDPOINT_FORMAT, scheme, host,
                    "10000", DEVSTORE_ACCOUNT_NAME));
            URI queuePrimaryEndpoint = new URI(String.format(DEVELOPMENT_STORAGE_PRIMARY_ENDPOINT_FORMAT, scheme, host,
                    "10001", DEVSTORE_ACCOUNT_NAME));
            URI tablePrimaryEndpoint = new URI(String.format(DEVELOPMENT_STORAGE_PRIMARY_ENDPOINT_FORMAT, scheme, host,
                    "10002", DEVSTORE_ACCOUNT_NAME));

            URI blobSecondaryEndpoint = new URI(String.format(DEVELOPMENT_STORAGE_SECONDARY_ENDPOINT_FORMAT, scheme, host,
                    "10000", DEVSTORE_ACCOUNT_NAME));
            URI queueSecondaryEndpoint = new URI(String.format(DEVELOPMENT_STORAGE_SECONDARY_ENDPOINT_FORMAT, scheme, host,
                    "10001", DEVSTORE_ACCOUNT_NAME));
            URI tableSecondaryEndpoint = new URI(String.format(DEVELOPMENT_STORAGE_SECONDARY_ENDPOINT_FORMAT, scheme, host,
                    "10002", DEVSTORE_ACCOUNT_NAME));

            CloudStorageAccount account = new CloudStorageAccount(credentials, new StorageUri(blobPrimaryEndpoint,
                    blobSecondaryEndpoint), new StorageUri(queuePrimaryEndpoint, queueSecondaryEndpoint), new StorageUri(
                    tablePrimaryEndpoint, tableSecondaryEndpoint), null );

            account.isDevStoreAccount = true;

            return account;
        }


        public static CloudStorageAccount parse(final String connectionString) throws URISyntaxException,
                InvalidKeyException {
            if (connectionString == null || connectionString.length() == 0) {
                throw new IllegalArgumentException(SR.INVALID_CONNECTION_STRING);
            }

            // 1. Parse connection string in to key / value pairs
            final Map<String, String> settings = Utility.parseAccountString(connectionString);

            // 2 Validate General Settings rules,
            // - only setting value per key
            // - setting must have value.
            //   - One special case to this rule - the account key can be empty.
            for (final Map.Entry<String, String> entry : settings.entrySet()) {
                if (entry.getValue() == null || entry.getValue().equals(Constants.EMPTY_STRING)) {
                    if (!entry.getKey().equals(CloudStorageAccount.ACCOUNT_KEY_NAME)) {
                        throw new IllegalArgumentException(SR.INVALID_CONNECTION_STRING);
                    }
                }
            }

            // 3. Validate scenario specific constraints
            CloudStorageAccount account = tryConfigureDevStore(settings);
            if (account != null) {
                return account;
            }

            account = tryConfigureServiceAccount(settings);
            if (account != null) {
                return account;
            }

            throw new IllegalArgumentException(SR.INVALID_CONNECTION_STRING);
        }


        private static StorageUri getStorageUri(final Map<String, String> settings,
                final String service, final String serviceEndpoint) throws URISyntaxException {

            // Explicit Endpoint Case
            if (settings.containsKey(serviceEndpoint)) {
                return new StorageUri(new URI(settings.get(serviceEndpoint)));
            }
            // Automatic Endpoint Case
            else if (settings.containsKey(DEFAULT_ENDPOINTS_PROTOCOL_NAME) &&
                     settings.containsKey(CloudStorageAccount.ACCOUNT_NAME_NAME) &&
                     settings.containsKey(CloudStorageAccount.ACCOUNT_KEY_NAME)) {
                final String scheme = settings.get(CloudStorageAccount.DEFAULT_ENDPOINTS_PROTOCOL_NAME);
                final String accountName = settings.get(CloudStorageAccount.ACCOUNT_NAME_NAME);
                final String endpointSuffix = settings.get(CloudStorageAccount.ENDPOINT_SUFFIX_NAME);
                return getDefaultStorageUri(scheme, accountName, getDNS(service, endpointSuffix));
            }
            // Otherwise
            else {
                return null;
            }
        }


        private static StorageUri getDefaultStorageUri(final String scheme, final String accountName,
                final String service) throws URISyntaxException {
            if (Utility.isNullOrEmpty(scheme)) {
                throw new IllegalArgumentException(SR.SCHEME_NULL_OR_EMPTY);
            }

            if (Utility.isNullOrEmpty(accountName)) {
                throw new IllegalArgumentException(SR.ACCOUNT_NAME_NULL_OR_EMPTY);
            }

            URI primaryUri = new URI(String.format(
                    PRIMARY_ENDPOINT_FORMAT, scheme, accountName, service));
            URI secondaryUri = new URI(String.format(
                    SECONDARY_ENDPOINT_FORMAT,scheme, accountName,
                    SECONDARY_LOCATION_ACCOUNT_SUFFIX, service));
            return new StorageUri(primaryUri, secondaryUri);
        }


        private static String getDNS(String service, String base) {
            if (base == null) {
                base = DEFAULT_DNS;
            }

            return String.format(DNS_NAME_FORMAT, service, base);
        }


        private static CloudStorageAccount tryConfigureDevStore(final Map<String, String> settings)
                throws URISyntaxException {
            if (settings.containsKey(USE_DEVELOPMENT_STORAGE_NAME)) {
                if (!Boolean.parseBoolean(settings.get(USE_DEVELOPMENT_STORAGE_NAME))) {
                    throw new IllegalArgumentException(SR.INVALID_CONNECTION_STRING_DEV_STORE_NOT_TRUE);
                }

                URI devStoreProxyUri = null;
                if (settings.containsKey(DEVELOPMENT_STORAGE_PROXY_URI_NAME)) {
                    devStoreProxyUri = new URI(settings.get(DEVELOPMENT_STORAGE_PROXY_URI_NAME));
                }

                return getDevelopmentStorageAccount(devStoreProxyUri);
            }
            else {
                return null;
            }
        }


        private static CloudStorageAccount tryConfigureServiceAccount(final Map<String, String> settings)
                throws URISyntaxException, InvalidKeyException {
            if (settings.containsKey(USE_DEVELOPMENT_STORAGE_NAME)) {
                if (!Boolean.parseBoolean(settings.get(USE_DEVELOPMENT_STORAGE_NAME))) {
                    throw new IllegalArgumentException(SR.INVALID_CONNECTION_STRING_DEV_STORE_NOT_TRUE);
                }
                else {
                    return null;
                }
            }

            String defaultEndpointSetting = settings.get(DEFAULT_ENDPOINTS_PROTOCOL_NAME);
            if (defaultEndpointSetting != null) {
                defaultEndpointSetting = defaultEndpointSetting.toLowerCase();
                if(!defaultEndpointSetting.equals(Constants.HTTP)
                        && !defaultEndpointSetting.equals(Constants.HTTPS)) {
                    return null;
                }
            }

            final StorageCredentials credentials = StorageCredentials.tryParseCredentials(settings);
            final CloudStorageAccount account = new CloudStorageAccount(credentials,
                    getStorageUri(settings, SR.BLOB, BLOB_ENDPOINT_NAME),
                    getStorageUri(settings, SR.QUEUE, QUEUE_ENDPOINT_NAME),
                    getStorageUri(settings, SR.TABLE, TABLE_ENDPOINT_NAME),
                    getStorageUri(settings, SR.FILE, FILE_ENDPOINT_NAME));

            // Invalid Account String
            if ((account.getBlobEndpoint() == null) && (account.getFileEndpoint() == null) &&
                    (account.getQueueEndpoint() == null) && (account.getTableEndpoint() == null)) {
                return null;
            }

            // Endpoint is only default if it is neither null nor explicitly specified
            account.isBlobEndpointDefault = !((account.getBlobEndpoint() == null) ||
                      settings.containsKey(CloudStorageAccount.BLOB_ENDPOINT_NAME));
            account.isFileEndpointDefault = !((account.getFileEndpoint() == null) ||
                      settings.containsKey(CloudStorageAccount.FILE_ENDPOINT_NAME));
            account.isQueueEndpointDefault = !((account.getQueueEndpoint() == null) ||
                      settings.containsKey(CloudStorageAccount.QUEUE_ENDPOINT_NAME));
            account.isTableEndpointDefault = !((account.getTableEndpoint() == null) ||
                      settings.containsKey(CloudStorageAccount.TABLE_ENDPOINT_NAME));

            account.endpointSuffix = settings.get(CloudStorageAccount.ENDPOINT_SUFFIX_NAME);

            return account;
        }



        private String endpointSuffix;


        private final StorageUri blobStorageUri;


        private final StorageUri fileStorageUri;


        private final StorageUri queueStorageUri;


        private final StorageUri tableStorageUri;


        private StorageCredentials credentials;


        private boolean isBlobEndpointDefault = false;


        private boolean isFileEndpointDefault = false;


        private boolean isQueueEndpointDefault = false;


        private boolean isTableEndpointDefault = false;


        private boolean isDevStoreAccount = false;


        public CloudStorageAccount(final StorageCredentials storageCredentials)
                throws URISyntaxException {
            // Protocol defaults to HTTP unless otherwise specified
            this(storageCredentials, false, null);
        }


        public CloudStorageAccount(final StorageCredentials storageCredentials,
                final boolean useHttps) throws URISyntaxException {
            this (storageCredentials, useHttps, null);
        }


        public CloudStorageAccount(final StorageCredentials storageCredentials,
                final boolean useHttps, final String endpointSuffix) throws URISyntaxException {
            Utility.assertNotNull("storageCredentials", storageCredentials);
            String protocol = useHttps ? Constants.HTTPS : Constants.HTTP;

            this.credentials = storageCredentials;
            this.blobStorageUri = getDefaultStorageUri(protocol, storageCredentials.getAccountName(),
                    getDNS(SR.BLOB, endpointSuffix));
            this.fileStorageUri = getDefaultStorageUri(protocol, storageCredentials.getAccountName(),
                    getDNS(SR.FILE, endpointSuffix));
            this.queueStorageUri = getDefaultStorageUri(protocol, storageCredentials.getAccountName(),
                    getDNS(SR.QUEUE, endpointSuffix));
            this.tableStorageUri = getDefaultStorageUri(protocol, storageCredentials.getAccountName(),
                    getDNS(SR.TABLE, endpointSuffix));
            this.endpointSuffix = endpointSuffix;

            this.isBlobEndpointDefault = true;
            this.isFileEndpointDefault = true;
            this.isQueueEndpointDefault = true;
            this.isTableEndpointDefault = true;
        }


        public CloudStorageAccount(final StorageCredentials storageCredentials, final URI blobEndpoint,
                final URI queueEndpoint, final URI tableEndpoint) {
            this(storageCredentials, new StorageUri(blobEndpoint), new StorageUri(queueEndpoint),
                    new StorageUri(tableEndpoint), null);
        }


        public CloudStorageAccount(final StorageCredentials storageCredentials, final URI blobEndpoint,
                final URI queueEndpoint, final URI tableEndpoint, final URI fileEndpoint) {
            this(storageCredentials, new StorageUri(blobEndpoint), new StorageUri(queueEndpoint),
                    new StorageUri(tableEndpoint), new StorageUri(fileEndpoint));
        }


        public CloudStorageAccount(final StorageCredentials storageCredentials,
                final StorageUri blobStorageUri,
                final StorageUri queueStorageUri,
                final StorageUri tableStorageUri) {
            this(storageCredentials, blobStorageUri, queueStorageUri, tableStorageUri, null);
        }


        public CloudStorageAccount(
                final StorageCredentials storageCredentials, final StorageUri blobStorageUri,
                final StorageUri queueStorageUri, final StorageUri tableStorageUri,
                final StorageUri fileStorageUri) {
            this.credentials = storageCredentials;
            this.blobStorageUri = blobStorageUri;
            this.fileStorageUri = fileStorageUri;
            this.queueStorageUri = queueStorageUri;
            this.tableStorageUri = tableStorageUri;
            this.endpointSuffix = null;
        }


        public CloudBlobClient createCloudBlobClient() {
            if (this.getBlobStorageUri() == null) {
                throw new IllegalArgumentException(SR.BLOB_ENDPOINT_NOT_CONFIGURED);
            }

            if (this.credentials == null) {
                throw new IllegalArgumentException(SR.MISSING_CREDENTIALS);
            }

            if (!StorageCredentialsHelper.canCredentialsSignRequest(this.credentials)) {
                throw new IllegalArgumentException(SR.CREDENTIALS_CANNOT_SIGN_REQUEST);
            }
            return new CloudBlobClient(this.getBlobStorageUri(), this.getCredentials());
        }


        public URI getBlobEndpoint() {
            if (this.getCredentials() instanceof StorageCredentialsSharedAccessSignature) {
                throw new IllegalArgumentException(SR.ENDPOINT_INFORMATION_UNAVAILABLE);
            }

            if (this.blobStorageUri == null) {
                return null;
            }

            return this.blobStorageUri.getPrimaryUri();
        }


        public StorageUri getBlobStorageUri() {
            if (this.getCredentials() instanceof StorageCredentialsSharedAccessSignature) {
                throw new IllegalArgumentException(SR.ENDPOINT_INFORMATION_UNAVAILABLE);
            }

            return this.blobStorageUri;
        }


        public StorageCredentials getCredentials() {
            return this.credentials;
        }


        public String getEndpointSuffix() {
            return this.endpointSuffix;
        }


        public URI getFileEndpoint() {
            if (this.getCredentials() instanceof StorageCredentialsSharedAccessSignature) {
                throw new IllegalArgumentException(SR.ENDPOINT_INFORMATION_UNAVAILABLE);
            }

            if (this.fileStorageUri == null) {
                return null;
            }

            return this.fileStorageUri.getPrimaryUri();
        }


        public StorageUri getFileStorageUri() {
            if (this.getCredentials() instanceof StorageCredentialsSharedAccessSignature) {
                throw new IllegalArgumentException(SR.ENDPOINT_INFORMATION_UNAVAILABLE);
            }

            return this.fileStorageUri;
        }


        public URI getQueueEndpoint() {
            if (this.getCredentials() instanceof StorageCredentialsSharedAccessSignature) {
                throw new IllegalArgumentException(SR.ENDPOINT_INFORMATION_UNAVAILABLE);
            }

            if (this.queueStorageUri == null) {
                return null;
            }

            return this.queueStorageUri.getPrimaryUri();
        }


        public StorageUri getQueueStorageUri() {
            if (this.getCredentials() instanceof StorageCredentialsSharedAccessSignature) {
                throw new IllegalArgumentException(SR.ENDPOINT_INFORMATION_UNAVAILABLE);
            }

            return this.queueStorageUri;
        }


        public URI getTableEndpoint() {
            if (this.getCredentials() instanceof StorageCredentialsSharedAccessSignature) {
                throw new IllegalArgumentException(SR.ENDPOINT_INFORMATION_UNAVAILABLE);
            }

            if (this.tableStorageUri == null) {
                return null;
            }

            return this.tableStorageUri.getPrimaryUri();
        }


        public StorageUri getTableStorageUri() {
            if (this.getCredentials() instanceof StorageCredentialsSharedAccessSignature) {
                throw new IllegalArgumentException(SR.ENDPOINT_INFORMATION_UNAVAILABLE);
            }

            return this.tableStorageUri;
        }


        @Override
        public String toString() {
            return this.toString(false);
        }


        public String toString(final boolean exportSecrets) {
            if (this.credentials != null && Utility.isNullOrEmpty(this.credentials.getAccountName())) {
                return this.credentials.toString(exportSecrets);
            }

            final List<String> values = new ArrayList<String>();
            if (this.isDevStoreAccount) {
                values.add(String.format("%s=true", USE_DEVELOPMENT_STORAGE_NAME));
                if (!this.getBlobEndpoint().toString().equals("http://127.0.0.1:10000/devstoreaccount1")) {
                    values.add(String.format("%s=%s://%s/", DEVELOPMENT_STORAGE_PROXY_URI_NAME,
                            this.getBlobEndpoint().getScheme(), this.getBlobEndpoint().getHost()));
                }
            }
            else {
                final String attributeFormat = "%s=%s";
                boolean addDefault = false;

                if (this.endpointSuffix != null) {
                    values.add(String.format(attributeFormat, ENDPOINT_SUFFIX_NAME, this.endpointSuffix));
                }

                if (this.getBlobStorageUri() != null) {
                    if (this.isBlobEndpointDefault) {
                        addDefault = true;
                    }
                    else {
                        values.add(String.format(attributeFormat, BLOB_ENDPOINT_NAME, this.getBlobEndpoint()));
                    }
                }

                if (this.getQueueStorageUri() != null) {
                    if (this.isQueueEndpointDefault) {
                        addDefault = true;
                    }
                    else {
                        values.add(String.format(attributeFormat, QUEUE_ENDPOINT_NAME, this.getQueueEndpoint()));
                    }
                }

                if (this.getTableStorageUri() != null) {
                    if (this.isTableEndpointDefault) {
                        addDefault = true;
                    }
                    else {
                        values.add(String.format(attributeFormat, TABLE_ENDPOINT_NAME, this.getTableEndpoint()));
                    }
                }

                if (this.getFileStorageUri() != null) {
                    if (this.isFileEndpointDefault) {
                        addDefault = true;
                    }
                    else {
                        values.add(String.format(attributeFormat, FILE_ENDPOINT_NAME, this.getFileEndpoint()));
                    }
                }

                if (addDefault) {
                    values.add(String.format(attributeFormat, DEFAULT_ENDPOINTS_PROTOCOL_NAME,
                            this.getBlobEndpoint().getScheme()));
                }

                if (this.getCredentials() != null) {
                    values.add(this.getCredentials().toString(exportSecrets));
                }
            }

            final StringBuilder returnString = new StringBuilder();
            for (final String val : values) {
                returnString.append(val);
                returnString.append(';');
            }

            // Remove trailing ';'
            if (values.size() > 0) {
                returnString.deleteCharAt(returnString.length() - 1);
            }

            return returnString.toString();
        }


        protected void setCredentials(final StorageCredentials credentials) {
            this.credentials = credentials;
        }
    }

    public static final class StorageKey {

        public static synchronized String computeMacSha256(final StorageKey storageKey, final String stringToSign)
                throws InvalidKeyException {
            if (storageKey.hmacSha256 == null) {
                storageKey.initHmacSha256();
            }

            byte[] utf8Bytes = null;
            try {
                utf8Bytes = stringToSign.getBytes(Constants.UTF8_CHARSET);
            }
            catch (final UnsupportedEncodingException e) {
                throw new IllegalArgumentException(e);
            }

            return Base64.encode(storageKey.hmacSha256.doFinal(utf8Bytes));
        }


        public static synchronized String computeMacSha512(final StorageKey storageKey, final String stringToSign)
                throws InvalidKeyException {
            if (storageKey.hmacSha512 == null) {
                storageKey.initHmacSha512();
            }

            byte[] utf8Bytes = null;
            try {
                utf8Bytes = stringToSign.getBytes(Constants.UTF8_CHARSET);
            }
            catch (final UnsupportedEncodingException e) {
                throw new IllegalArgumentException(e);
            }

            return Base64.encode(storageKey.hmacSha512.doFinal(utf8Bytes));
        }


        private Mac hmacSha256;


        private Mac hmacSha512;


        private SecretKey key256;


        private SecretKey key512;


        private byte[] key;


        public StorageKey(final byte[] key) {
            this.setKey(key);
        }


        public String getBase64EncodedKey() {
            return Base64.encode(this.key);
        }


        public byte[] getKey() {
            final byte[] copy = this.key.clone();
            return copy;
        }


        private void initHmacSha256() throws InvalidKeyException {
            this.key256 = new SecretKeySpec(this.key, "HmacSHA256");
            try {
                this.hmacSha256 = Mac.getInstance("HmacSHA256");
            }
            catch (final NoSuchAlgorithmException e) {
                throw new IllegalArgumentException();
            }
            this.hmacSha256.init(this.key256);
        }


        private void initHmacSha512() throws InvalidKeyException {
            this.key512 = new SecretKeySpec(this.key, "HmacSHA512");
            try {
                this.hmacSha512 = Mac.getInstance("HmacSHA512");
            }
            catch (final NoSuchAlgorithmException e) {
                throw new IllegalArgumentException();
            }
            this.hmacSha512.init(this.key512);
        }


        public void setKey(final byte[] key) {
            this.key = key;
            this.hmacSha256 = null;
            this.hmacSha512 = null;
            this.key256 = null;
            this.key512 = null;
        }


        public void setKey(final String key) {
            this.key = Base64.decode(key);
        }
    }

    public static class StorageException extends Exception {

        private static final long serialVersionUID = 7972747254288274928L;


        public static StorageException translateClientException(final Exception cause) {
            return new StorageException("Client error",
                    "A Client side exception occurred, please check the inner exception for details",
                    Constants.HeaderConstants.HTTP_UNUSED_306, null, cause);
        }


        public static StorageException translateException(final StorageRequest<?, ?, ?> request, final Exception cause,
                final OperationContext opContext) {
            if (request == null || request.getConnection() == null) {
                return translateClientException(cause);
            }

            if (cause instanceof SocketException) {
                String message = cause == null ? Constants.EMPTY_STRING : cause.getMessage();
                return new StorageException(StorageErrorCode.SERVICE_INTERNAL_ERROR.toString(),
                        "An unknown failure occurred : ".concat(message), HttpURLConnection.HTTP_INTERNAL_ERROR,
                        null, cause);
            }

            StorageException translatedException = null;

            String responseMessage = null;
            int responseCode = 0;
            try {
                responseCode = request.getConnection().getResponseCode();
                responseMessage = request.getConnection().getResponseMessage();
            } catch (final IOException e) {
                // ignore errors
            }

            if (responseMessage == null) {
                responseMessage = Constants.EMPTY_STRING;
            }

            StorageExtendedErrorInformation extendedError = request.parseErrorDetails();
            if (extendedError != null) {
                // 1. If extended information is available use it
                translatedException = new StorageException(extendedError.getErrorCode(), responseMessage, responseCode,
                        extendedError, cause);
            } else {
                // 2. If extended information is unavailable, translate exception based
                // on status code
                translatedException = translateFromHttpStatus(responseCode, responseMessage, cause);
            }

            if (translatedException != null) {
                Utility.logHttpError(translatedException, opContext);
                return translatedException;
            } else {
                return new StorageException(StorageErrorCode.SERVICE_INTERNAL_ERROR.toString(),
                        "The server encountered an unknown failure: ".concat(responseMessage),
                        HttpURLConnection.HTTP_INTERNAL_ERROR, null, cause);
            }
        }


        protected static StorageException translateFromHttpStatus(final int statusCode, final String statusDescription,
                final Exception inner) {
            String errorCode;
            switch (statusCode) {
            case HttpURLConnection.HTTP_FORBIDDEN:
                errorCode = StorageErrorCode.ACCESS_DENIED.toString();
                break;
            case HttpURLConnection.HTTP_GONE:
            case HttpURLConnection.HTTP_NOT_FOUND:
                errorCode = StorageErrorCode.RESOURCE_NOT_FOUND.toString();
                break;
            case 416:
            case HttpURLConnection.HTTP_BAD_REQUEST:
                // 416: RequestedRangeNotSatisfiable - No corresponding enum in HttpURLConnection
                errorCode = StorageErrorCode.BAD_REQUEST.toString();
                break;

            case HttpURLConnection.HTTP_PRECON_FAILED:
            case HttpURLConnection.HTTP_NOT_MODIFIED:
                errorCode = StorageErrorCode.CONDITION_FAILED.toString();
                break;

            case HttpURLConnection.HTTP_CONFLICT:
                errorCode = StorageErrorCode.RESOURCE_ALREADY_EXISTS.toString();
                break;

            case HttpURLConnection.HTTP_UNAVAILABLE:
                errorCode = StorageErrorCode.SERVER_BUSY.toString();
                break;

            case HttpURLConnection.HTTP_GATEWAY_TIMEOUT:
                errorCode = StorageErrorCode.SERVICE_TIMEOUT.toString();
                break;

            case HttpURLConnection.HTTP_INTERNAL_ERROR:
                errorCode = StorageErrorCode.SERVICE_INTERNAL_ERROR.toString();
                break;

            case HttpURLConnection.HTTP_NOT_IMPLEMENTED:
                errorCode = StorageErrorCode.NOT_IMPLEMENTED.toString();
                break;

            case HttpURLConnection.HTTP_BAD_GATEWAY:
                errorCode = StorageErrorCode.BAD_GATEWAY.toString();
                break;

            case HttpURLConnection.HTTP_VERSION:
                errorCode = StorageErrorCode.HTTP_VERSION_NOT_SUPPORTED.toString();
                break;
            default:
                errorCode = null;
            }

            if (errorCode == null) {
                return null;
            } else {
                return new StorageException(errorCode, statusDescription, statusCode, null, inner);
            }
        }


        protected String errorCode;


        protected StorageExtendedErrorInformation extendedErrorInformation;


        private final int httpStatusCode;


        public StorageException(final String errorCode, final String message, final int statusCode,
                final StorageExtendedErrorInformation extendedErrorInfo, final Exception innerException) {
            super(message, innerException);
            this.errorCode = errorCode;
            this.httpStatusCode = statusCode;
            this.extendedErrorInformation = extendedErrorInfo;
        }


        public String getErrorCode() {
            return this.errorCode;
        }


        public StorageExtendedErrorInformation getExtendedErrorInformation() {
            return this.extendedErrorInformation;
        }


        public int getHttpStatusCode() {
            return this.httpStatusCode;
        }
    }

    public static final class Credentials {

        private String accountName;


        private final StorageKey key;


        private String keyName;


        public Credentials(final String accountName, final byte[] key) {
            if (accountName == null || accountName.length() == 0) {
                throw new IllegalArgumentException(SR.INVALID_ACCOUNT_NAME);
            }

            if (key == null) {
                throw new IllegalArgumentException(SR.KEY_NULL);
            }

            this.accountName = accountName;
            this.key = new StorageKey(key);
        }


        public Credentials(final String accountName, final String key) {
            this(accountName, Base64.decode(key));
        }


        public String exportBase64EncodedKey() {
            return this.getKey().getBase64EncodedKey();
        }


        public byte[] exportKey() {
            return this.getKey().getKey();
        }


        public String getAccountName() {
            return this.accountName;
        }


        public String getKeyName() {
            return this.keyName;
        }


        public StorageKey getKey() {
            return this.key;
        }


        protected void setAccountName(final String accountName) {
            this.accountName = accountName;
        }


        protected void setKeyName(final String keyName) {
            this.keyName = keyName;
        }
    }

    public static final class CorsProperties {


        private List<CorsRule> corsRules = new ArrayList<CorsRule>();


        public List<CorsRule> getCorsRules() {
            return this.corsRules;
        }


        protected void setCorsRules(List<CorsRule> corsRules) {
            this.corsRules = corsRules;
        }

    }

    public static final class RetryingEvent extends BaseEvent {


        private final RetryContext retryContext;


        public RetryingEvent(OperationContext opContext, Object connectionObject, RequestResult requestResult,
                RetryContext retryContext) {
            super(opContext, connectionObject, requestResult);
            this.retryContext = retryContext;
        }


        public RetryContext getRetryContext() {
            return this.retryContext;
        }

    }

    public static interface RetryPolicyFactory {


        RetryPolicy createInstance(OperationContext opContext);
    }

    public static final class StorageCredentialsSharedAccessSignature extends StorageCredentials {


        private final String token;


        public StorageCredentialsSharedAccessSignature(final String token) {
            this.token = token;
        }


        public String getToken() {
            return this.token;
        }


        @Override
        public String toString(final boolean exportSecrets) {
            return String.format("%s=%s", CloudStorageAccount.SHARED_ACCESS_SIGNATURE_NAME, exportSecrets ? this.token
                    : "[signature hidden]");
        }


        @Override
        public URI transformUri(final URI resourceUri, final OperationContext opContext) throws URISyntaxException,
                StorageException {
            if (resourceUri == null) {
                return null;
            }

            // append the sas token to the resource uri
            URI sasUri = PathUtility.addToQuery(resourceUri, this.token);

            // append the api version parameter to the sas uri
            String apiVersion = Constants.QueryConstants.API_VERSION + "=" + Constants.HeaderConstants.TARGET_STORAGE_VERSION;
            return PathUtility.addToQuery(sasUri, apiVersion);
        }


        @Override
        public StorageUri transformUri(StorageUri resourceUri, OperationContext opContext) throws URISyntaxException,
                StorageException {
            return new StorageUri(this.transformUri(resourceUri.getPrimaryUri(), opContext), this.transformUri(
                    resourceUri.getSecondaryUri(), opContext));
        }
    }

    public abstract static class Permissions<T extends SharedAccessPolicy> {

        private HashMap<String, T> sharedAccessPolicies;


        public Permissions() {
            this.sharedAccessPolicies = new HashMap<String, T>();
        }


        public HashMap<String, T> getSharedAccessPolicies() {
            return this.sharedAccessPolicies;
        }


        public void setSharedAccessPolicies(final HashMap<String, T> sharedAccessPolicies) {
            this.sharedAccessPolicies = sharedAccessPolicies;
        }
    }

    public static final class StorageEventMultiCaster<EVENT_TYPE extends BaseEvent, EVENT_LISTENER_TYPE extends StorageEvent<EVENT_TYPE>> {


        private final CopyOnWriteArrayList<EVENT_LISTENER_TYPE> listeners = new CopyOnWriteArrayList<EVENT_LISTENER_TYPE>();


        public void addListener(final EVENT_LISTENER_TYPE listener) {
            this.listeners.add(listener);
        }


        public void fireEvent(final EVENT_TYPE event) {
            for (final StorageEvent<EVENT_TYPE> listener : this.listeners) {
                listener.eventOccurred(event);
            }
        }


        public boolean hasListeners() {
            return this.listeners.size() > 0;
        }


        public void removeListener(final EVENT_LISTENER_TYPE listener) {
            this.listeners.remove(listener);
        }
    }

    public static enum ResultContinuationType {

        NONE,


        BLOB,


        CONTAINER,


        FILE,


        QUEUE,


        TABLE,


        SHARE,
    }

    public static enum LocationMode {

        PRIMARY_ONLY,


        PRIMARY_THEN_SECONDARY,


        SECONDARY_ONLY,


        SECONDARY_THEN_PRIMARY;
    }

    public static final class OperationContext {


        public static final String defaultLoggerName =  "AzureClient";


        private static boolean enableLoggingByDefault = false;


        private long clientTimeInMs;


        private String clientRequestID;


        private Boolean enableLogging;


        private Log logger;


        private final ArrayList<RequestResult> requestResults;


        private HashMap<String, String> userHeaders;


        private static StorageEventMultiCaster<SendingRequestEvent, StorageEvent<SendingRequestEvent>> globalSendingRequestEventHandler = new StorageEventMultiCaster<SendingRequestEvent, StorageEvent<SendingRequestEvent>>();


        private static StorageEventMultiCaster<ResponseReceivedEvent, StorageEvent<ResponseReceivedEvent>> globalResponseReceivedEventHandler = new StorageEventMultiCaster<ResponseReceivedEvent, StorageEvent<ResponseReceivedEvent>>();


        private static StorageEventMultiCaster<RequestCompletedEvent, StorageEvent<RequestCompletedEvent>> globalRequestCompletedEventHandler = new StorageEventMultiCaster<RequestCompletedEvent, StorageEvent<RequestCompletedEvent>>();


        private static StorageEventMultiCaster<RetryingEvent, StorageEvent<RetryingEvent>> globalRetryingEventHandler = new StorageEventMultiCaster<RetryingEvent, StorageEvent<RetryingEvent>>();


        private StorageEventMultiCaster<SendingRequestEvent, StorageEvent<SendingRequestEvent>> sendingRequestEventHandler = new StorageEventMultiCaster<SendingRequestEvent, StorageEvent<SendingRequestEvent>>();


        private StorageEventMultiCaster<ResponseReceivedEvent, StorageEvent<ResponseReceivedEvent>> responseReceivedEventHandler = new StorageEventMultiCaster<ResponseReceivedEvent, StorageEvent<ResponseReceivedEvent>>();


        private StorageEventMultiCaster<RequestCompletedEvent, StorageEvent<RequestCompletedEvent>> requestCompletedEventHandler = new StorageEventMultiCaster<RequestCompletedEvent, StorageEvent<RequestCompletedEvent>>();


        private StorageEventMultiCaster<RetryingEvent, StorageEvent<RetryingEvent>> retryingEventHandler = new StorageEventMultiCaster<RetryingEvent, StorageEvent<RetryingEvent>>();


        public OperationContext() {
            this.clientRequestID = UUID.randomUUID().toString();
            this.requestResults = new ArrayList<RequestResult>();
        }


        public String getClientRequestID() {
            return this.clientRequestID;
        }


        public long getClientTimeInMs() {
            return this.clientTimeInMs;
        }


        public synchronized RequestResult getLastResult() {
            if (this.requestResults == null || this.requestResults.size() == 0) {
                return null;
            }
            else {
                return this.requestResults.get(this.requestResults.size() - 1);
            }
        }


        public Log getLogger() {
            if (this.logger == null) {
                setDefaultLoggerSynchronized();
            }

            return this.logger;
        }


        public HashMap<String, String> getUserHeaders() {
            return this.userHeaders;
        }


        private synchronized void setDefaultLoggerSynchronized() {
            if (this.logger == null) {
                this.logger = LogFactory.getLog(OperationContext.defaultLoggerName);
            }
        }


        public ArrayList<RequestResult> getRequestResults() {
            return this.requestResults;
        }


        public synchronized void appendRequestResult(RequestResult requestResult) {
            this.requestResults.add(requestResult);
        }


        public static StorageEventMultiCaster<SendingRequestEvent, StorageEvent<SendingRequestEvent>> getGlobalSendingRequestEventHandler() {
            return OperationContext.globalSendingRequestEventHandler;
        }


        public static StorageEventMultiCaster<ResponseReceivedEvent, StorageEvent<ResponseReceivedEvent>> getGlobalResponseReceivedEventHandler() {
            return OperationContext.globalResponseReceivedEventHandler;
        }


        public static StorageEventMultiCaster<RequestCompletedEvent, StorageEvent<RequestCompletedEvent>> getGlobalRequestCompletedEventHandler() {
            return OperationContext.globalRequestCompletedEventHandler;
        }


        public static StorageEventMultiCaster<RetryingEvent, StorageEvent<RetryingEvent>> getGlobalRetryingEventHandler() {
            return OperationContext.globalRetryingEventHandler;
        }


        public StorageEventMultiCaster<SendingRequestEvent, StorageEvent<SendingRequestEvent>> getSendingRequestEventHandler() {
            return this.sendingRequestEventHandler;
        }


        public StorageEventMultiCaster<ResponseReceivedEvent, StorageEvent<ResponseReceivedEvent>> getResponseReceivedEventHandler() {
            return this.responseReceivedEventHandler;
        }


        public StorageEventMultiCaster<RequestCompletedEvent, StorageEvent<RequestCompletedEvent>> getRequestCompletedEventHandler() {
            return this.requestCompletedEventHandler;
        }


        public StorageEventMultiCaster<RetryingEvent, StorageEvent<RetryingEvent>> getRetryingEventHandler() {
            return this.retryingEventHandler;
        }


        public void initialize() {
            this.setClientTimeInMs(0);
            this.requestResults.clear();
        }


        public boolean isLoggingEnabled() {
            if (this.enableLogging == null) {
                return enableLoggingByDefault;
            }
            return this.enableLogging;
        }


        public void setClientRequestID(final String clientRequestID) {
            this.clientRequestID = clientRequestID;
        }


        public void setClientTimeInMs(final long clientTimeInMs) {
            this.clientTimeInMs = clientTimeInMs;
        }


        public void setLogger(final Log logger) {
            this.logger = logger;
        }


        public void setUserHeaders(final HashMap<String, String> userHeaders) {
            this.userHeaders = userHeaders;
        }


        public void setLoggingEnabled(boolean loggingEnabled) {
            this.enableLogging = loggingEnabled;
        }


        public static void setGlobalSendingRequestEventHandler(
                final StorageEventMultiCaster<SendingRequestEvent, StorageEvent<SendingRequestEvent>> globalSendingRequestEventHandler) {
            OperationContext.globalSendingRequestEventHandler = globalSendingRequestEventHandler;
        }


        public static void setGlobalResponseReceivedEventHandler(
                final StorageEventMultiCaster<ResponseReceivedEvent, StorageEvent<ResponseReceivedEvent>> globalResponseReceivedEventHandler) {
            OperationContext.globalResponseReceivedEventHandler = globalResponseReceivedEventHandler;
        }


        public static void setGlobalRequestCompletedEventHandler(
                final StorageEventMultiCaster<RequestCompletedEvent, StorageEvent<RequestCompletedEvent>> globalRequestCompletedEventHandler) {
            OperationContext.globalRequestCompletedEventHandler = globalRequestCompletedEventHandler;
        }


        public static void setGlobalRetryingEventHandler(
                final StorageEventMultiCaster<RetryingEvent, StorageEvent<RetryingEvent>> globalRetryingEventHandler) {
            OperationContext.globalRetryingEventHandler = globalRetryingEventHandler;
        }


        public void setSendingRequestEventHandler(
                final StorageEventMultiCaster<SendingRequestEvent, StorageEvent<SendingRequestEvent>> sendingRequestEventHandler) {
            this.sendingRequestEventHandler = sendingRequestEventHandler;
        }


        public void setResponseReceivedEventHandler(
                final StorageEventMultiCaster<ResponseReceivedEvent, StorageEvent<ResponseReceivedEvent>> responseReceivedEventHandler) {
            this.responseReceivedEventHandler = responseReceivedEventHandler;
        }


        public void setRequestCompletedEventHandler(
                final StorageEventMultiCaster<RequestCompletedEvent, StorageEvent<RequestCompletedEvent>> requestCompletedEventHandler) {
            this.requestCompletedEventHandler = requestCompletedEventHandler;
        }


        public void setRetryingEventHandler(
                final StorageEventMultiCaster<RetryingEvent, StorageEvent<RetryingEvent>> retryingEventHandler) {
            this.retryingEventHandler = retryingEventHandler;
        }


        public static boolean isLoggingEnabledByDefault() {
            return enableLoggingByDefault;
        }


        public static void setLoggingEnabledByDefault(boolean enableLoggingByDefault) {
            OperationContext.enableLoggingByDefault = enableLoggingByDefault;
        }
    }

    static final class ServicePropertiesHandler extends DefaultHandler {

        private final Stack<String> elementStack = new Stack<String>();
        private StringBuilder bld = new StringBuilder();

        private final ServiceProperties props = new ServiceProperties();

        private CorsRule rule = new CorsRule();
        private boolean retentionPolicyEnabled;
        private int retentionPolicyDays;


        public static ServiceProperties readServicePropertiesFromStream(final InputStream stream) throws SAXException,
                IOException, ParserConfigurationException {
            SAXParser saxParser = Utility.getSAXParser();
            ServicePropertiesHandler handler = new ServicePropertiesHandler();
            saxParser.parse(stream, handler);

            return handler.props;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            this.elementStack.push(localName);

            if (Constants.AnalyticsConstants.CORS_RULE_ELEMENT.equals(localName)) {
                this.rule = new CorsRule();
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            String currentNode = this.elementStack.pop();

            // if the node popped from the stack and the localName don't match, the xml document is improperly formatted
            if (!localName.equals(currentNode)) {
                throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
            }

            String parentNode = null;
            if (!this.elementStack.isEmpty()) {
                parentNode = this.elementStack.peek();
            }

            String value = this.bld.toString();
            if (value.isEmpty()) {
                value = null;
            }

            if (this.retentionPolicyEnabled && Constants.AnalyticsConstants.RETENTION_POLICY_ELEMENT.equals(currentNode)) {
                if (Constants.AnalyticsConstants.LOGGING_ELEMENT.equals(parentNode)) {
                    this.props.getLogging().setRetentionIntervalInDays(this.retentionPolicyDays);
                }
                else if (Constants.AnalyticsConstants.HOUR_METRICS_ELEMENT.equals(parentNode)) {
                    this.props.getHourMetrics().setRetentionIntervalInDays(this.retentionPolicyDays);
                }
                else if (Constants.AnalyticsConstants.MINUTE_METRICS_ELEMENT.equals(parentNode)) {
                    this.props.getMinuteMetrics().setRetentionIntervalInDays(this.retentionPolicyDays);
                }
            }
            else if (Constants.AnalyticsConstants.CORS_RULE_ELEMENT.equals(currentNode)) {
                this.props.getCors().getCorsRules().add(this.rule);
            }
            else if (Constants.AnalyticsConstants.RETENTION_POLICY_ELEMENT.equals(parentNode)) {
                if (Constants.AnalyticsConstants.DAYS_ELEMENT.equals(currentNode)) {
                    this.retentionPolicyDays = Integer.parseInt(value);
                }
                else if (Constants.AnalyticsConstants.ENABLED_ELEMENT.equals(currentNode)) {
                    this.retentionPolicyEnabled = Boolean.parseBoolean(value);
                }
            }
            else if (Constants.AnalyticsConstants.LOGGING_ELEMENT.equals(parentNode)) {
                if (Constants.AnalyticsConstants.VERSION_ELEMENT.equals(currentNode)) {
                    this.props.getLogging().setVersion(value);
                }
                else if (Constants.AnalyticsConstants.DELETE_ELEMENT.equals(currentNode)) {
                    if (Boolean.parseBoolean(value)) {
                        this.props.getLogging().getLogOperationTypes().add(LoggingOperations.DELETE);
                    }
                }
                else if (Constants.AnalyticsConstants.READ_ELEMENT.equals(currentNode)) {
                    if (Boolean.parseBoolean(value)) {
                        this.props.getLogging().getLogOperationTypes().add(LoggingOperations.READ);
                    }
                }
                else if (Constants.AnalyticsConstants.WRITE_ELEMENT.equals(currentNode)) {
                    if (Boolean.parseBoolean(value)) {
                        this.props.getLogging().getLogOperationTypes().add(LoggingOperations.WRITE);
                    }
                }
            }
            else if (Constants.AnalyticsConstants.HOUR_METRICS_ELEMENT.equals(parentNode)) {
                MetricsProperties metrics = this.props.getHourMetrics();
                if (Constants.AnalyticsConstants.VERSION_ELEMENT.equals(currentNode)) {
                    metrics.setVersion(value);
                }
                else if (Constants.AnalyticsConstants.ENABLED_ELEMENT.equals(currentNode)) {
                    if (Boolean.parseBoolean(value)) {
                        metrics.setMetricsLevel(metrics.getMetricsLevel() != MetricsLevel.SERVICE_AND_API ? MetricsLevel.SERVICE
                                : MetricsLevel.SERVICE_AND_API);
                    }
                }
                else if (Constants.AnalyticsConstants.INCLUDE_APIS_ELEMENT.equals(currentNode)) {
                    if (Boolean.parseBoolean(value) && metrics.getMetricsLevel() != MetricsLevel.DISABLED) {
                        metrics.setMetricsLevel(MetricsLevel.SERVICE_AND_API);
                    }
                }
            }
            else if (Constants.AnalyticsConstants.MINUTE_METRICS_ELEMENT.equals(parentNode)) {
                MetricsProperties metrics = this.props.getMinuteMetrics();
                if (Constants.AnalyticsConstants.VERSION_ELEMENT.equals(currentNode)) {
                    metrics.setVersion(value);
                }
                else if (Constants.AnalyticsConstants.ENABLED_ELEMENT.equals(currentNode)) {
                    if (Boolean.parseBoolean(value)) {
                        metrics.setMetricsLevel(metrics.getMetricsLevel() != MetricsLevel.SERVICE_AND_API ? MetricsLevel.SERVICE
                                : MetricsLevel.SERVICE_AND_API);
                    }
                }
                else if (Constants.AnalyticsConstants.INCLUDE_APIS_ELEMENT.equals(currentNode)) {
                    if (Boolean.parseBoolean(value) && metrics.getMetricsLevel() != MetricsLevel.DISABLED) {
                        metrics.setMetricsLevel(MetricsLevel.SERVICE_AND_API);
                    }
                }
            }
            else if (Constants.AnalyticsConstants.DEFAULT_SERVICE_VERSION.equals(currentNode)) {
                this.props.setDefaultServiceVersion(value);
            }
            else if (Constants.AnalyticsConstants.CORS_RULE_ELEMENT.equals(parentNode)) {
                if (Constants.AnalyticsConstants.ALLOWED_ORIGINS_ELEMENT.equals(currentNode)) {
                    if (value != null) {
                        this.rule.setAllowedOrigins(splitToList(value, ","));
                    }
                }
                else if (Constants.AnalyticsConstants.ALLOWED_METHODS_ELEMENT.equals(currentNode)) {
                    if (value != null) {
                        this.rule.setAllowedMethods(splitToEnumSet(value, ","));
                    }
                }
                else if (Constants.AnalyticsConstants.EXPOSED_HEADERS_ELEMENT.equals(currentNode)) {
                    if (value != null) {
                        this.rule.setExposedHeaders(splitToList(value, ","));
                    }
                }
                else if (Constants.AnalyticsConstants.ALLOWED_HEADERS_ELEMENT.equals(currentNode)) {
                    if (value != null) {
                        this.rule.setAllowedHeaders(splitToList(value, ","));
                    }
                }
                else if (Constants.AnalyticsConstants.MAX_AGE_IN_SECONDS_ELEMENT.equals(currentNode)) {
                    this.rule.setMaxAgeInSeconds(Integer.parseInt(value));
                }
            }

            this.bld = new StringBuilder();
        }

        @Override
        public void characters(char ch[], int start, int length) throws SAXException {
            this.bld.append(ch, start, length);
        }


        private static List<String> splitToList(String str, String delimiter) {
            ArrayList<String> list = new ArrayList<String>();
            StringTokenizer st = new StringTokenizer(str, delimiter);
            while (st.hasMoreElements()) {
                list.add(st.nextToken());
            }
            return list;
        }


        private static EnumSet<CorsHttpMethods> splitToEnumSet(String str, String delimiter) {
            EnumSet<CorsHttpMethods> set = EnumSet.noneOf(CorsHttpMethods.class);
            StringTokenizer st = new StringTokenizer(str, delimiter);
            while (st.hasMoreElements()) {
                set.add(CorsHttpMethods.valueOf(st.nextToken()));
            }
            return set;
        }
    }

    public static final class ResultContinuation {

        private String nextMarker;


        private String nextPartitionKey;


        private String nextRowKey;


        private String nextTableName;


        private ResultContinuationType continuationType;


        private StorageLocation targetLocation;


        public ResultContinuation() {
            // Empty default constructor.
        }


        public ResultContinuationType getContinuationType() {
            return this.continuationType;
        }


        public String getNextMarker() {
            return this.nextMarker;
        }


        public String getNextPartitionKey() {
            return this.nextPartitionKey;
        }


        public String getNextRowKey() {
            return this.nextRowKey;
        }


        public String getNextTableName() {
            return this.nextTableName;
        }


        public StorageLocation getTargetLocation() {
            return this.targetLocation;
        }


        public boolean hasContinuation() {
            return this.getNextMarker() != null || this.nextPartitionKey != null || this.nextRowKey != null
                    || this.nextTableName != null;
        }


        public void setContinuationType(final ResultContinuationType continuationType) {
            this.continuationType = continuationType;
        }


        public void setNextMarker(final String nextMarker) {
            this.nextMarker = nextMarker;
        }


        public void setNextPartitionKey(final String nextPartitionKey) {
            this.nextPartitionKey = nextPartitionKey;
        }


        public void setNextRowKey(final String nextRowKey) {
            this.nextRowKey = nextRowKey;
        }


        public void setNextTableName(final String nextTableName) {
            this.nextTableName = nextTableName;
        }


        public void setTargetLocation(StorageLocation targetLocation) {
            this.targetLocation = targetLocation;
        }
    }

    public static class StorageUri {

        private static boolean AreUrisEqual(URI uri1, URI uri2) {
            return uri1 == null ? uri2 == null : uri1.equals(uri2);
        }

        private static void AssertAbsoluteUri(URI uri) {
            if ((uri != null) && !uri.isAbsolute()) {
                String errorMessage = String.format(Utility.LOCALE_US, SR.RELATIVE_ADDRESS_NOT_PERMITTED, uri.toString());
                throw new IllegalArgumentException(errorMessage);
            }
        }

        private URI primaryUri;

        private URI secondaryUri;


        public StorageUri(URI primaryUri) {
            this(primaryUri, null );
        }


        public StorageUri(URI primaryUri, URI secondaryUri) {
            if (primaryUri == null && secondaryUri == null) {
                throw new IllegalArgumentException(SR.STORAGE_URI_NOT_NULL);
            }
            if (primaryUri != null && secondaryUri != null) {
                // check query component is equivalent
                if ((primaryUri.getQuery() == null && secondaryUri.getQuery() != null)
                        || (primaryUri.getQuery() != null && !(primaryUri.getQuery().equals(secondaryUri.getQuery())))) {
                    throw new IllegalArgumentException(SR.STORAGE_URI_MUST_MATCH);
                }

                boolean primaryPathStyle = Utility.determinePathStyleFromUri(primaryUri);
                boolean secondaryPathStyle = Utility.determinePathStyleFromUri(secondaryUri);

                if (!primaryPathStyle && !secondaryPathStyle) {
                    if ((primaryUri.getPath() == null && secondaryUri.getPath() != null)
                            || (primaryUri.getPath() != null && !(primaryUri.getPath().equals(secondaryUri.getPath())))) {
                        throw new IllegalArgumentException(SR.STORAGE_URI_MUST_MATCH);
                    }
                }
                else {
                    final int maxPrimaryPathSegments = primaryPathStyle ? 3 : 2;
                    final int maxSecondaryPathSegments = secondaryPathStyle ? 3 : 2;

                    // getPath() on a path-style uri returns /devstore1[/path1/path2], split(3) returns ["","devstore","path1/path2"]
                    // getPath() on a regular uri returns [/path1/path2], split(2) returns ["","path1/path2"]
                    final String[] primaryPathSegments = primaryUri.getPath().split("/", maxPrimaryPathSegments);
                    final String[] secondaryPathSegments = secondaryUri.getPath().split("/", maxSecondaryPathSegments);

                    String primaryPath = "";
                    if (primaryPathSegments.length == maxPrimaryPathSegments) {
                        primaryPath = primaryPathSegments[primaryPathSegments.length - 1];
                    }

                    String secondaryPath = "";
                    if (secondaryPathSegments.length == maxSecondaryPathSegments) {
                        secondaryPath = secondaryPathSegments[secondaryPathSegments.length - 1];
                    }

                    if (!primaryPath.equals(secondaryPath)) {
                        throw new IllegalArgumentException(SR.STORAGE_URI_MUST_MATCH);
                    }
                }
            }

            this.setPrimaryUri(primaryUri);
            this.setSecondaryUri(secondaryUri);
        }

        @Override
        public boolean equals(Object obj) {
            return this.equals((StorageUri) obj);
        }


        public boolean equals(StorageUri other) {
            return (other != null) && StorageUri.AreUrisEqual(this.primaryUri, other.primaryUri)
                    && StorageUri.AreUrisEqual(this.secondaryUri, other.secondaryUri);
        }


        public URI getPrimaryUri() {
            return this.primaryUri;
        }


        public URI getSecondaryUri() {
            return this.secondaryUri;
        }


        public URI getUri(StorageLocation location) {
            switch (location) {
                case PRIMARY:
                    return this.primaryUri;

                case SECONDARY:
                    return this.secondaryUri;

                default:
                    throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.ARGUMENT_OUT_OF_RANGE_ERROR,
                            "location", location.toString()));
            }
        }

        @Override
        public int hashCode() {
            int hash1 = this.primaryUri != null ? this.primaryUri.hashCode() : 0;
            int hash2 = this.secondaryUri != null ? this.secondaryUri.hashCode() : 0;
            return hash1 ^ hash2;
        }


        private void setPrimaryUri(URI primaryUri) {
            StorageUri.AssertAbsoluteUri(primaryUri);
            this.primaryUri = primaryUri;
        }


        private void setSecondaryUri(URI secondaryUri) {
            StorageUri.AssertAbsoluteUri(secondaryUri);
            this.secondaryUri = secondaryUri;
        }

        @Override
        public String toString() {
            return String.format(Utility.LOCALE_US, "Primary = '%s'; Secondary = '%s'", this.primaryUri, this.secondaryUri);
        }


        public boolean validateLocationMode(LocationMode mode) {
            switch (mode) {
                case PRIMARY_ONLY:
                    return this.primaryUri != null;

                case SECONDARY_ONLY:
                    return this.secondaryUri != null;

                default:
                    return (this.primaryUri != null) && (this.secondaryUri != null);
            }
        }


        public boolean isAbsolute() {
            if (this.secondaryUri == null) {
                return this.primaryUri.isAbsolute();
            }
            else {
                return this.primaryUri.isAbsolute() && this.secondaryUri.isAbsolute();
            }
        }


        public String getQuery() {
            return this.primaryUri.getQuery();
        }
    }

    public static final class StorageCredentialsAnonymous extends StorageCredentials {


        public static final StorageCredentials ANONYMOUS = new StorageCredentialsAnonymous();


        protected static StorageCredentials getInstance() {
            return StorageCredentialsAnonymous.ANONYMOUS;
        }


        protected StorageCredentialsAnonymous() {
            // Empty Default Ctor
        }


        @Override
        public String toString(final boolean exportSecrets) {
            return Constants.EMPTY_STRING;
        }

        @Override
        public URI transformUri(URI resourceUri, OperationContext opContext) {
            return resourceUri;
        }

        @Override
        public StorageUri transformUri(StorageUri resourceUri, OperationContext opContext) {
            return resourceUri;
        }
    }

    public static final class RequestResult {


        private Exception exception;


        private String serviceRequestID;


        private String contentMD5;


        private String requestDate;


        private String etag;


        private Date startDate;


        private int statusCode = -1;


        private String statusMessage;


        private Date stopDate;


        private StorageLocation targetLocation;


        public StorageLocation getTargetLocation() {
            return this.targetLocation;
        }


        public String getContentMD5() {
            return this.contentMD5;
        }


        public String getEtag() {
            return this.etag;
        }


        public Exception getException() {
            return this.exception;
        }


        public String getRequestDate() {
            return this.requestDate;
        }


        public String getServiceRequestID() {
            return this.serviceRequestID;
        }


        public Date getStartDate() {
            return this.startDate;
        }


        public int getStatusCode() {
            return this.statusCode;
        }


        public String getStatusMessage() {
            return this.statusMessage;
        }


        public Date getStopDate() {
            return this.stopDate;
        }


        public void setContentMD5(final String contentMD5) {
            this.contentMD5 = contentMD5;
        }


        public void setEtag(final String etag) {
            this.etag = etag;
        }


        public void setException(final Exception exception) {
            this.exception = exception;
        }


        public void setRequestDate(final String requestDate) {
            this.requestDate = requestDate;
        }


        public void setServiceRequestID(final String serviceRequestID) {
            this.serviceRequestID = serviceRequestID;
        }


        public void setStartDate(final Date startDate) {
            this.startDate = startDate;
        }


        public void setStatusCode(final int statusCode) {
            this.statusCode = statusCode;
        }


        public void setStatusMessage(final String statusMessage) {
            this.statusMessage = statusMessage;
        }


        public void setStopDate(final Date stopDate) {
            this.stopDate = stopDate;
        }


        public void setTargetLocation(StorageLocation targetLocation) {
            this.targetLocation = targetLocation;
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    public static @interface DoesServiceRequest {
        // No attributes
    }

    public static class GeoReplicationStats {


        private GeoReplicationStatus status;


        private Date lastSyncTime;


        GeoReplicationStats() {
            // no op
        }


        public Date getLastSyncTime() {
            return this.lastSyncTime;
        }


        public GeoReplicationStatus getStatus() {
            return this.status;
        }


        void setLastSyncTime(Date lastSyncTime) {
            this.lastSyncTime = lastSyncTime;
        }


        void setStatus(GeoReplicationStatus status) {
            this.status = status;
        }
    }

    public static class RetryInfo {


        private StorageLocation targetLocation;


        private LocationMode updatedLocationMode;


        private int retryInterval = 3000;


        public RetryInfo() {
            this.targetLocation = StorageLocation.PRIMARY;
            this.updatedLocationMode = LocationMode.PRIMARY_ONLY;
        }


        public RetryInfo(RetryContext retryContext) {
            Utility.assertNotNull("retryContext", retryContext);
            this.targetLocation = retryContext.getNextLocation();
            this.updatedLocationMode = retryContext.getLocationMode();
        }


        public int getRetryInterval() {
            return this.retryInterval;
        }


        public final StorageLocation getTargetLocation() {
            return this.targetLocation;
        }


        public LocationMode getUpdatedLocationMode() {
            return this.updatedLocationMode;
        }


        public void setRetryInterval(int retryInterval) {
            this.retryInterval = (retryInterval > 0 ? retryInterval : 0);
        }


        public void setTargetLocation(StorageLocation targetLocation) {
            this.targetLocation = targetLocation;
        }


        public void setUpdatedLocationMode(LocationMode updatedLocationMode) {
            this.updatedLocationMode = updatedLocationMode;
        }


        @Override
        public String toString() {
            return String.format(Utility.LOCALE_US, "(%s,%s)", this.targetLocation, this.retryInterval);
        }

    }

    public static class NameValidator {
        private static final int BLOB_FILE_DIRECTORY_MIN_LENGTH = 1;
        private static final int CONTAINER_SHARE_QUEUE_TABLE_MIN_LENGTH = 3;
        private static final int CONTAINER_SHARE_QUEUE_TABLE_MAX_LENGTH = 63;
        private static final int FILE_DIRECTORY_MAX_LENGTH = 255;
        private static final int BLOB_MAX_LENGTH = 1024;
        private static final Pattern FILE_DIRECTORY_REGEX = Pattern.compile("^[^\"\\/:|<>*?]*/{0,1}");
        private static final Pattern SHARE_CONTAINER_QUEUE_REGEX = Pattern.compile("^[a-z0-9]+(-[a-z0-9]+)*$");
        private static final Pattern TABLE_REGEX = Pattern.compile("^[A-Za-z][A-Za-z0-9]*$");
        private static final Pattern METRICS_TABLE_REGEX = Pattern.compile("^\\$Metrics(HourPrimary|MinutePrimary|HourSecondary|MinuteSecondary)?(Transactions)(Blob|Queue|Table)$");
        private static final String[] RESERVED_FILE_NAMES = { ".", "..", "LPT1",
                "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
                "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
                "COM9", "PRN", "AUX", "NUL", "CON", "CLOCK$" };


        public static void validateContainerName(String containerName) {
            if (!("$root".equals(containerName) || "$logs".equals(containerName))) {
                NameValidator.validateShareContainerQueueHelper(containerName, SR.CONTAINER);
            }
        }


        public static void validateQueueName(String queueName) {
            NameValidator.validateShareContainerQueueHelper(queueName, SR.QUEUE);
        }


        public static void validateShareName(String shareName) {
            NameValidator.validateShareContainerQueueHelper(shareName, SR.SHARE);
        }

        private static void validateShareContainerQueueHelper(String resourceName,
                String resourceType) {
            if (Utility.isNullOrEmptyOrWhitespace(resourceName)) {
                throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.RESOURCE_NAME_EMPTY, resourceType));
            }

            if (resourceName.length() < NameValidator.CONTAINER_SHARE_QUEUE_TABLE_MIN_LENGTH || resourceName.length() > NameValidator.CONTAINER_SHARE_QUEUE_TABLE_MAX_LENGTH) {
                throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.INVALID_RESOURCE_NAME_LENGTH, resourceType, NameValidator.CONTAINER_SHARE_QUEUE_TABLE_MIN_LENGTH, NameValidator.CONTAINER_SHARE_QUEUE_TABLE_MAX_LENGTH));
            }

            if (!NameValidator.SHARE_CONTAINER_QUEUE_REGEX.matcher(resourceName).matches())
            {
                throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.INVALID_RESOURCE_NAME, resourceType));
            }
        }


        public static void validateBlobName(String blobName) {
            if (Utility.isNullOrEmptyOrWhitespace(blobName)) {
                throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.RESOURCE_NAME_EMPTY, SR.BLOB));
            }

            if (blobName.length() < NameValidator.BLOB_FILE_DIRECTORY_MIN_LENGTH || blobName.length() > NameValidator.BLOB_MAX_LENGTH) {
                throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.INVALID_RESOURCE_NAME_LENGTH, SR.BLOB, NameValidator.BLOB_FILE_DIRECTORY_MIN_LENGTH, NameValidator.BLOB_MAX_LENGTH));
            }

            int slashCount =  0;
            for (int i = 0; i < blobName.length(); i++)
            {
                if (blobName.charAt(i) == '/')
                {
                    slashCount++;
                }
            }

            if (slashCount >= 254)
            {
                throw new IllegalArgumentException(SR.TOO_MANY_PATH_SEGMENTS);
            }
        }


        public static void validateFileName(String fileName) {
            NameValidator.ValidateFileDirectoryHelper(fileName, SR.FILE);

            if (fileName.endsWith("/")) {
                throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.INVALID_RESOURCE_NAME, SR.FILE));
            }

            for (String s : NameValidator.RESERVED_FILE_NAMES) {
                if (s.equalsIgnoreCase(fileName)) {
                    throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.INVALID_RESOURCE_RESERVED_NAME, SR.FILE));
                }
            }
        }


        public static void validateDirectoryName(String directoryName) {
            NameValidator.ValidateFileDirectoryHelper(directoryName, SR.DIRECTORY);
        }

        private static void ValidateFileDirectoryHelper(String resourceName, String resourceType) {
            if (Utility.isNullOrEmptyOrWhitespace(resourceName)) {
                throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.RESOURCE_NAME_EMPTY, resourceType));
            }

            if (resourceName.length() < NameValidator.BLOB_FILE_DIRECTORY_MIN_LENGTH || resourceName.length() > NameValidator.FILE_DIRECTORY_MAX_LENGTH) {
                throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.INVALID_RESOURCE_NAME_LENGTH, resourceType, NameValidator.BLOB_FILE_DIRECTORY_MIN_LENGTH, NameValidator.FILE_DIRECTORY_MAX_LENGTH ));
            }

            if (!NameValidator.FILE_DIRECTORY_REGEX.matcher(resourceName).matches()) {
                throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.INVALID_RESOURCE_NAME, resourceType));
            }
        }


        public static void validateTableName(String tableName) {
            if (Utility.isNullOrEmptyOrWhitespace(tableName)) {
                throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.RESOURCE_NAME_EMPTY, SR.TABLE));
            }

            if (tableName.length() < NameValidator.CONTAINER_SHARE_QUEUE_TABLE_MIN_LENGTH || tableName.length() > NameValidator.CONTAINER_SHARE_QUEUE_TABLE_MAX_LENGTH) {
                throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.INVALID_RESOURCE_NAME_LENGTH, SR.TABLE, NameValidator.CONTAINER_SHARE_QUEUE_TABLE_MIN_LENGTH, NameValidator.CONTAINER_SHARE_QUEUE_TABLE_MAX_LENGTH));
            }

            if (!(NameValidator.TABLE_REGEX.matcher(tableName).matches()
                    || NameValidator.METRICS_TABLE_REGEX.matcher(tableName).matches()
                    || tableName.equalsIgnoreCase("$MetricsCapacityBlob"))) {
                throw new IllegalArgumentException(String.format(Utility.LOCALE_US, SR.INVALID_RESOURCE_NAME, SR.TABLE));
            }
        }
    }

    public static final class LoggingProperties {


        private String version = "1.0";


        private EnumSet<LoggingOperations> logOperationTypes = EnumSet.noneOf(LoggingOperations.class);


        private Integer retentionIntervalInDays;


        public EnumSet<LoggingOperations> getLogOperationTypes() {
            return this.logOperationTypes;
        }


        public Integer getRetentionIntervalInDays() {
            return this.retentionIntervalInDays;
        }


        public String getVersion() {
            return this.version;
        }


        public void setLogOperationTypes(final EnumSet<LoggingOperations> logOperationTypes) {
            this.logOperationTypes = logOperationTypes;
        }


        public void setRetentionIntervalInDays(final Integer retentionIntervalInDays) {
            this.retentionIntervalInDays = retentionIntervalInDays;
        }


        public void setVersion(final String version) {
            this.version = version;
        }
    }

    public static final class SharedAccessPolicySerializer {

        public static <T extends SharedAccessPolicy> void writeSharedAccessIdentifiersToStream(
                final HashMap<String, T> sharedAccessPolicies, final StringWriter outWriter) throws XMLStreamException {
            Utility.assertNotNull("sharedAccessPolicies", sharedAccessPolicies);
            Utility.assertNotNull("outWriter", outWriter);

            final XMLStreamWriter xmlw = Utility.createXMLStreamWriter(outWriter);

            if (sharedAccessPolicies.keySet().size() > Constants.MAX_SHARED_ACCESS_POLICY_IDENTIFIERS) {
                final String errorMessage = String.format(SR.TOO_MANY_SHARED_ACCESS_POLICY_IDENTIFIERS,
                        sharedAccessPolicies.keySet().size(), Constants.MAX_SHARED_ACCESS_POLICY_IDENTIFIERS);

                throw new IllegalArgumentException(errorMessage);
            }

            // default is UTF8
            xmlw.writeStartDocument();
            xmlw.writeStartElement(Constants.SIGNED_IDENTIFIERS_ELEMENT);

            for (final Map.Entry<String, T> entry : sharedAccessPolicies.entrySet()) {
                final SharedAccessPolicy policy = entry.getValue();
                xmlw.writeStartElement(Constants.SIGNED_IDENTIFIER_ELEMENT);

                // Set the identifier
                xmlw.writeStartElement(Constants.ID);
                xmlw.writeCharacters(entry.getKey());
                xmlw.writeEndElement();

                xmlw.writeStartElement(Constants.ACCESS_POLICY);

                // Set the Start Time
                xmlw.writeStartElement(Constants.START);
                xmlw.writeCharacters(Utility.getUTCTimeOrEmpty(policy.getSharedAccessStartTime()));
                // end Start
                xmlw.writeEndElement();

                // Set the Expiry Time
                xmlw.writeStartElement(Constants.EXPIRY);
                xmlw.writeCharacters(Utility.getUTCTimeOrEmpty(policy.getSharedAccessExpiryTime()));
                // end Expiry
                xmlw.writeEndElement();

                // Set the Permissions
                xmlw.writeStartElement(Constants.PERMISSION);
                xmlw.writeCharacters(policy.permissionsToString());
                // end Permission
                xmlw.writeEndElement();

                // end AccessPolicy
                xmlw.writeEndElement();
                // end SignedIdentifier
                xmlw.writeEndElement();
            }

            // end SignedIdentifiers
            xmlw.writeEndElement();
            // end doc
            xmlw.writeEndDocument();
        }
    }

    public static enum MetricsLevel {

        DISABLED,


        SERVICE,


        SERVICE_AND_API;
    }

    public static enum StorageErrorCode {

        ACCESS_DENIED(12),


        ACCOUNT_NOT_FOUND(8),


        AUTHENTICATION_FAILURE(11),


        BAD_GATEWAY(18),


        BAD_REQUEST(16),


        BLOB_ALREADY_EXISTS(15),


        BLOB_NOT_FOUND(10),


        CONDITION_FAILED(17),


        CONTAINER_ALREADY_EXISTS(14),


        CONTAINER_NOT_FOUND(9),


        HTTP_VERSION_NOT_SUPPORTED(20),


        NONE(0),


        NOT_IMPLEMENTED(19),


        RESOURCE_ALREADY_EXISTS(13),


        RESOURCE_NOT_FOUND(7),


        SERVICE_BAD_REQUEST(6),


        SERVICE_INTEGRITY_CHECK_FAILED(4),


        SERVICE_INTERNAL_ERROR(1),


        SERVICE_TIMEOUT(3),


        TRANSPORT_ERROR(5),


        LEASE_ID_MISSING(21),


        LEASE_ID_MISMATCH(22),


        LEASE_NOT_PRESENT(23),


        SERVER_BUSY(24);


        public int value;


        StorageErrorCode(final int val) {
            this.value = val;
        }
    }

    public static enum LoggingOperations {

        READ,


        WRITE,


        DELETE;
    }

    public abstract static class ServiceClient {


        private StorageUri storageUri;


        protected StorageCredentials credentials;


        private boolean usePathStyleUris;


        @SuppressWarnings("deprecation")
        protected AuthenticationScheme authenticationScheme = AuthenticationScheme.SHAREDKEYFULL;


        protected ServiceClient(final StorageUri storageUri, final StorageCredentials credentials) {
            Utility.assertNotNull("baseUri", storageUri);
            if (!storageUri.isAbsolute()) {
                throw new IllegalArgumentException(String.format(SR.RELATIVE_ADDRESS_NOT_PERMITTED, storageUri));
            }

            this.credentials = credentials == null ? StorageCredentialsAnonymous.ANONYMOUS : credentials;
            this.usePathStyleUris = Utility.determinePathStyleFromUri(storageUri.getPrimaryUri());
            this.storageUri = storageUri;
        }

        protected StorageRequest<ServiceClient, Void, ServiceProperties> downloadServicePropertiesImpl(
                final RequestOptions options, final boolean signAsTable) {
            final StorageRequest<ServiceClient, Void, ServiceProperties> getRequest = new StorageRequest<ServiceClient, Void, ServiceProperties>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(ServiceClient client, Void parentObject, OperationContext context)
                        throws Exception {
                    return BaseRequest.getServiceProperties(client.getEndpoint(), options, null, context);
                }

                @Override
                public void signRequest(HttpURLConnection connection, ServiceClient client, OperationContext context)
                        throws Exception {
                    if (signAsTable) {
                        StorageRequest.signTableRequest(connection, client, -1, null);
                    }
                    else {
                        StorageRequest.signBlobQueueAndFileRequest(connection, client, -1, null);
                    }
                }

                @Override
                public ServiceProperties preProcessResponse(Void parentObject, ServiceClient client,
                        OperationContext context) throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                    }

                    return null;
                }

                @Override
                public ServiceProperties postProcessResponse(HttpURLConnection connection, Void parentObject,
                        ServiceClient client, OperationContext context, ServiceProperties storageObject) throws Exception {
                    return ServicePropertiesHandler.readServicePropertiesFromStream(connection.getInputStream());
                }
            };

            return getRequest;
        }

        protected StorageRequest<ServiceClient, Void, ServiceStats> getServiceStatsImpl(final RequestOptions options,
                final boolean signAsTable) {
            final StorageRequest<ServiceClient, Void, ServiceStats> getRequest = new StorageRequest<ServiceClient, Void, ServiceStats>(
                    options, this.getStorageUri()) {

                @Override
                public void setRequestLocationMode() {
                    this.applyLocationModeToRequest();
                    this.setRequestLocationMode(RequestLocationMode.PRIMARY_OR_SECONDARY);
                }

                @Override
                public HttpURLConnection buildRequest(ServiceClient client, Void parentObject, OperationContext context)
                        throws Exception {
                    return BaseRequest.getServiceStats(client.getStorageUri().getUri(this.getCurrentLocation()), options,
                            null, context);
                }

                @Override
                public void signRequest(HttpURLConnection connection, ServiceClient client, OperationContext context)
                        throws Exception {
                    if (signAsTable) {
                        StorageRequest.signTableRequest(connection, client, -1, null);
                    }
                    else {
                        StorageRequest.signBlobQueueAndFileRequest(connection, client, -1, null);
                    }
                }

                @Override
                public ServiceStats preProcessResponse(Void parentObject, ServiceClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                        this.setNonExceptionedRetryableFailure(true);
                    }

                    return null;
                }

                @Override
                public ServiceStats postProcessResponse(HttpURLConnection connection, Void parentObject,
                        ServiceClient client, OperationContext context, ServiceStats storageObject) throws Exception {
                    return ServiceStatsHandler.readServiceStatsFromStream(connection.getInputStream());
                }

            };

            return getRequest;
        }


        public final StorageCredentials getCredentials() {
            return this.credentials;
        }


        public final AuthenticationScheme getAuthenticationScheme() {
            return this.authenticationScheme;
        }


        public final URI getEndpoint() {
            return this.storageUri.getPrimaryUri();
        }


        public final StorageUri getStorageUri() {
            return this.storageUri;
        }


        protected boolean isUsePathStyleUris() {
            return this.usePathStyleUris;
        }


        protected final void setCredentials(final StorageCredentials credentials) {
            this.credentials = credentials;
        }


        protected final void setStorageUri(final StorageUri storageUri) {
            this.usePathStyleUris = Utility.determinePathStyleFromUri(storageUri.getPrimaryUri());
            this.storageUri = storageUri;
        }


        public final void setAuthenticationScheme(final AuthenticationScheme scheme) {
            this.authenticationScheme = scheme;
        }

        protected StorageRequest<ServiceClient, Void, Void> uploadServicePropertiesImpl(final ServiceProperties properties,
                final RequestOptions options, final OperationContext opContext, final boolean signAsTable)
                throws StorageException {
            try {
                byte[] propertiesBytes = ServicePropertiesSerializer.serializeToByteArray(properties);

                final ByteArrayInputStream sendStream = new ByteArrayInputStream(propertiesBytes);
                final StreamMd5AndLength descriptor = Utility.analyzeStream(sendStream, -1L, -1L,
                        true, true);

                final StorageRequest<ServiceClient, Void, Void> putRequest = new StorageRequest<ServiceClient, Void, Void>(
                        options, this.getStorageUri()) {

                    @Override
                    public HttpURLConnection buildRequest(ServiceClient client, Void parentObject, OperationContext context)
                            throws Exception {
                        this.setSendStream(sendStream);
                        this.setLength(descriptor.getLength());
                        return BaseRequest.setServiceProperties(client.getEndpoint(), options, null, context);
                    }

                    @Override
                    public void setHeaders(HttpURLConnection connection, Void parentObject, OperationContext context) {
                        connection.setRequestProperty(Constants.HeaderConstants.CONTENT_MD5, descriptor.getMd5());
                    }

                    @Override
                    public void signRequest(HttpURLConnection connection, ServiceClient client, OperationContext context)
                            throws Exception {
                        if (signAsTable) {
                            StorageRequest.signTableRequest(connection, client, descriptor.getLength(), null);
                        }
                        else {
                            StorageRequest.signBlobQueueAndFileRequest(connection, client, descriptor.getLength(), null);
                        }
                    }

                    @Override
                    public Void preProcessResponse(Void parentObject, ServiceClient client, OperationContext context)
                            throws Exception {
                        if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_ACCEPTED) {
                            this.setNonExceptionedRetryableFailure(true);
                        }

                        return null;
                    }

                    @Override
                    public void recoveryAction(OperationContext context) throws IOException {
                        sendStream.reset();
                        sendStream.mark(Constants.MAX_MARK_LENGTH);
                    }
                };

                return putRequest;
            }
            catch (IllegalArgumentException e) {
                // to do : Move this to multiple catch clause so we can avoid the duplicated code once we move to Java 1.7.
                // The request was not even made. There was an error while trying to read the permissions. Just throw.
                StorageException translatedException = StorageException.translateClientException(e);
                throw translatedException;
            }
            catch (XMLStreamException e) {
                // The request was not even made. There was an error while trying to read the serviceProperties and write to stream. Just throw.
                StorageException translatedException = StorageException.translateClientException(e);
                throw translatedException;
            }
            catch (IOException e) {
                // The request was not even made. There was an error while trying to read the serviceProperties and write to stream. Just throw.
                StorageException translatedException = StorageException.translateClientException(e);
                throw translatedException;
            }
        }


        @Deprecated
        public final LocationMode getLocationMode() {
            return this.getDefaultRequestOptions().getLocationMode();
        }


        @Deprecated
        public final RetryPolicyFactory getRetryPolicyFactory() {
            return this.getDefaultRequestOptions().getRetryPolicyFactory();
        }


        @Deprecated
        public final int getTimeoutInMs() {
            return this.getDefaultRequestOptions().getTimeoutIntervalInMs();
        }


        @Deprecated
        public Integer getMaximumExecutionTimeInMs() {
            return this.getDefaultRequestOptions().getMaximumExecutionTimeInMs();
        }


        @Deprecated
        public void setLocationMode(LocationMode locationMode) {
            this.getDefaultRequestOptions().setLocationMode(locationMode);
        }


        @Deprecated
        public void setRetryPolicyFactory(final RetryPolicyFactory retryPolicyFactory) {
            this.getDefaultRequestOptions().setRetryPolicyFactory(retryPolicyFactory);
        }


        @Deprecated
        public final void setTimeoutInMs(final int timeoutInMs) {
            this.getDefaultRequestOptions().setTimeoutIntervalInMs(timeoutInMs);
        }


        @Deprecated
        public void setMaximumExecutionTimeInMs(Integer maximumExecutionTimeInMs) {
            this.getDefaultRequestOptions().setMaximumExecutionTimeInMs(maximumExecutionTimeInMs);
        }


        public abstract RequestOptions getDefaultRequestOptions();
    }

    public static final class StorageCredentialsAccountAndKey extends StorageCredentials {


        private Credentials credentials;


        public StorageCredentialsAccountAndKey(final String accountName, final byte[] key) {
            this.credentials = new Credentials(accountName, key);
        }


        public StorageCredentialsAccountAndKey(final String accountName, final String key) {
            this(accountName, Base64.decode(key));
        }


        @Override
        public String getAccountName() {
            return this.credentials.getAccountName();
        }


        public String getAccountKeyName() {
            return this.credentials.getKeyName();
        }


        public Credentials getCredentials() {
            return this.credentials;
        }


        public void setCredentials(final Credentials credentials) {
            this.credentials = credentials;
        }


        @Override
        public String toString(final boolean exportSecrets) {
            return String.format("%s=%s;%s=%s", CloudStorageAccount.ACCOUNT_NAME_NAME, this.getAccountName(),
                    CloudStorageAccount.ACCOUNT_KEY_NAME, exportSecrets ? this.credentials.getKey().getBase64EncodedKey()
                            : "[key hidden]");
        }

        @Override
        public URI transformUri(URI resourceUri, OperationContext opContext) {
            return resourceUri;
        }

        @Override
        public StorageUri transformUri(StorageUri resourceUri, OperationContext opContext) {
            return resourceUri;
        }
    }

    public abstract static class BaseEvent {

        private final Object connectionObject;


        private final OperationContext opContext;


        private final RequestResult requestResult;


        public BaseEvent(final OperationContext opContext, final Object connectionObject, final RequestResult requestResult) {
            this.opContext = opContext;
            this.connectionObject = connectionObject;
            this.requestResult = requestResult;
        }


        public Object getConnectionObject() {
            return this.connectionObject;
        }


        public OperationContext getOpContext() {
            return this.opContext;
        }


        public RequestResult getRequestResult() {
            return this.requestResult;
        }
    }

    public static final class RetryLinearRetry extends RetryPolicy implements RetryPolicyFactory {


        public RetryLinearRetry() {
            this(RetryPolicy.DEFAULT_CLIENT_BACKOFF, RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT);
        }


        public RetryLinearRetry(final int deltaBackoff, final int maxAttempts) {
            super(deltaBackoff, maxAttempts);
        }


        @Override
        public RetryPolicy createInstance(final OperationContext opContext) {
            return new RetryLinearRetry(this.deltaBackoffIntervalInMs, this.maximumAttempts);
        }


        @Override
        public RetryInfo evaluate(RetryContext retryContext, OperationContext operationContext) {

            boolean secondaryNotFound = this.evaluateLastAttemptAndSecondaryNotFound(retryContext);

            if (retryContext.getCurrentRetryCount() < this.maximumAttempts) {
                if ((!secondaryNotFound && retryContext.getLastRequestResult().getStatusCode() >= 400 && retryContext
                        .getLastRequestResult().getStatusCode() < 500)
                        || retryContext.getLastRequestResult().getStatusCode() == HttpURLConnection.HTTP_NOT_IMPLEMENTED
                        || retryContext.getLastRequestResult().getStatusCode() == HttpURLConnection.HTTP_VERSION) {
                    return null;
                }

                final long retryInterval = Math.max(
                        Math.min(this.deltaBackoffIntervalInMs, RetryPolicy.DEFAULT_MAX_BACKOFF),
                        RetryPolicy.DEFAULT_MIN_BACKOFF);

                return this.evaluateRetryInfo(retryContext, secondaryNotFound, retryInterval);
            }

            return null;
        }
    }

    public static final class RetryExponentialRetry extends RetryPolicy implements RetryPolicyFactory {


        private final Random randRef = new Random();


        private int resolvedMaxBackoff = RetryPolicy.DEFAULT_MAX_BACKOFF;


        private int resolvedMinBackoff = RetryPolicy.DEFAULT_MIN_BACKOFF;


        public RetryExponentialRetry() {
            this(RetryPolicy.DEFAULT_CLIENT_BACKOFF, RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT);
        }


        public RetryExponentialRetry(final int deltaBackoff, final int maxAttempts) {
            super(deltaBackoff, maxAttempts);
        }


        public RetryExponentialRetry(final int minBackoff, final int deltaBackoff, final int maxBackOff,
                final int maxAttempts) {
            super(deltaBackoff, maxAttempts);
            this.resolvedMinBackoff = minBackoff;
            this.resolvedMaxBackoff = maxBackOff;
        }


        @Override
        public RetryPolicy createInstance(final OperationContext opContext) {
            return new RetryExponentialRetry(this.resolvedMinBackoff, this.deltaBackoffIntervalInMs,
                    this.resolvedMaxBackoff, this.maximumAttempts);
        }


        @Override
        public RetryInfo evaluate(RetryContext retryContext, OperationContext operationContext) {

            boolean secondaryNotFound = this.evaluateLastAttemptAndSecondaryNotFound(retryContext);

            if (retryContext.getCurrentRetryCount() < this.maximumAttempts) {
                if ((!secondaryNotFound && retryContext.getLastRequestResult().getStatusCode() >= 400 && retryContext
                        .getLastRequestResult().getStatusCode() < 500)
                        || retryContext.getLastRequestResult().getStatusCode() == HttpURLConnection.HTTP_NOT_IMPLEMENTED
                        || retryContext.getLastRequestResult().getStatusCode() == HttpURLConnection.HTTP_VERSION) {
                    return null;
                }

                // Calculate backoff Interval between 80% and 120% of the desired
                // backoff, multiply by 2^n -1 for exponential
                double incrementDelta = (Math.pow(2, retryContext.getCurrentRetryCount()) - 1);
                final int boundedRandDelta = (int) (this.deltaBackoffIntervalInMs * 0.8)
                        + this.randRef.nextInt((int) (this.deltaBackoffIntervalInMs * 1.2)
                                - (int) (this.deltaBackoffIntervalInMs * 0.8));
                incrementDelta *= boundedRandDelta;

                final long retryInterval = (int) Math.round(Math.min(this.resolvedMinBackoff + incrementDelta,
                        this.resolvedMaxBackoff));

                return this.evaluateRetryInfo(retryContext, secondaryNotFound, retryInterval);
            }

            return null;
        }
    }

    public static final class Constants {

        public static class AnalyticsConstants {


            public static final String ALLOWED_HEADERS_ELEMENT = "AllowedHeaders";


            public static final String ALLOWED_METHODS_ELEMENT = "AllowedMethods";


            public static final String ALLOWED_ORIGINS_ELEMENT = "AllowedOrigins";


            public static final String CORS_ELEMENT = "Cors";


            public static final String CORS_RULE_ELEMENT = "CorsRule";


            public static final String DAYS_ELEMENT = "Days";


            public static final String DEFAULT_SERVICE_VERSION = "DefaultServiceVersion";


            public static final String DELETE_ELEMENT = "Delete";


            public static final String ENABLED_ELEMENT = "Enabled";


            public static final String EXPOSED_HEADERS_ELEMENT = "ExposedHeaders";


            public static final String HOUR_METRICS_ELEMENT = "HourMetrics";


            public static final String INCLUDE_APIS_ELEMENT = "IncludeAPIs";


            public static final String LOGS_CONTAINER = "$logs";


            public static final String LOGGING_ELEMENT = "Logging";


            public static final String MAX_AGE_IN_SECONDS_ELEMENT = "MaxAgeInSeconds";


            public static final String METRICS_CAPACITY_BLOB = "$MetricsCapacityBlob";


            public static final String METRICS_HOUR_PRIMARY_TRANSACTIONS_BLOB = "$MetricsHourPrimaryTransactionsBlob";


            public static final String METRICS_HOUR_PRIMARY_TRANSACTIONS_TABLE = "$MetricsHourPrimaryTransactionsTable";


            public static final String METRICS_HOUR_PRIMARY_TRANSACTIONS_QUEUE = "$MetricsHourPrimaryTransactionsQueue";


            public static final String METRICS_MINUTE_PRIMARY_TRANSACTIONS_BLOB = "$MetricsMinutePrimaryTransactionsBlob";


            public static final String METRICS_MINUTE_PRIMARY_TRANSACTIONS_TABLE = "$MetricsMinutePrimaryTransactionsTable";


            public static final String METRICS_MINUTE_PRIMARY_TRANSACTIONS_QUEUE = "$MetricsMinutePrimaryTransactionsQueue";


            public static final String METRICS_HOUR_SECONDARY_TRANSACTIONS_BLOB = "$MetricsHourSecondaryTransactionsBlob";


            public static final String METRICS_HOUR_SECONDARY_TRANSACTIONS_TABLE = "$MetricsHourSecondaryTransactionsTable";


            public static final String METRICS_HOUR_SECONDARY_TRANSACTIONS_QUEUE = "$MetricsHourSecondaryTransactionsQueue";


            public static final String METRICS_MINUTE_SECONDARY_TRANSACTIONS_BLOB = "$MetricsMinuteSecondaryTransactionsBlob";


            public static final String METRICS_MINUTE_SECONDARY_TRANSACTIONS_TABLE = "$MetricsMinuteSecondaryTransactionsTable";


            public static final String METRICS_MINUTE_SECONDARY_TRANSACTIONS_QUEUE = "$MetricsMinuteSecondaryTransactionsQueue";


            public static final String MINUTE_METRICS_ELEMENT = "MinuteMetrics";


            public static final String READ_ELEMENT = "Read";


            public static final String RETENTION_POLICY_ELEMENT = "RetentionPolicy";


            public static final String STORAGE_SERVICE_PROPERTIES_ELEMENT = "StorageServiceProperties";


            public static final String STORAGE_SERVICE_STATS = "StorageServiceStats";


            public static final String VERSION_ELEMENT = "Version";


            public static final String WRITE_ELEMENT = "Write";

        }


        public static class HeaderConstants {

            public static final String ACCEPT = "Accept";


            public static final String ACCEPT_CHARSET = "Accept-Charset";


            public static final String AUTHORIZATION = "Authorization";


            public static final String BEGIN_RANGE_HEADER_FORMAT = "bytes=%d-";


            public static final String BLOB_SEQUENCE_NUMBER = PREFIX_FOR_STORAGE_HEADER + "blob-sequence-number";


            public static final String CACHE_CONTROL = "Cache-Control";


            public static final String CACHE_CONTROL_HEADER = PREFIX_FOR_STORAGE_HEADER + "blob-cache-control";


            public static final String CLIENT_REQUEST_ID_HEADER = PREFIX_FOR_STORAGE_HEADER + "client-request-id";


            public static final String CONTENT_DISPOSITION = "Content-Disposition";


            public static final String CONTENT_ENCODING = "Content-Encoding";


            public static final String CONTENT_LANGUAGE = "Content-Language";


            public static final String CONTENT_LENGTH = "Content-Length";


            public static final String CONTENT_MD5 = "Content-MD5";


            public static final String CONTENT_RANGE = "Content-Range";


            public static final String CONTENT_TYPE = "Content-Type";


            public static final String COPY_ACTION_ABORT = "abort";


            public static final String COPY_ACTION_HEADER = PREFIX_FOR_STORAGE_HEADER + "copy-action";


            public static final String COPY_COMPLETION_TIME = PREFIX_FOR_STORAGE_HEADER + "copy-completion-time";


            public static final String COPY_ID = PREFIX_FOR_STORAGE_HEADER + "copy-id";


            public static final String COPY_PROGRESS = PREFIX_FOR_STORAGE_HEADER + "copy-progress";


            public static final String COPY_SOURCE = PREFIX_FOR_STORAGE_HEADER + "copy-source";


            public static final String COPY_SOURCE_HEADER = PREFIX_FOR_STORAGE_HEADER + "copy-source";


            public static final String COPY_STATUS = PREFIX_FOR_STORAGE_HEADER + "copy-status";


            public static final String COPY_STATUS_DESCRIPTION = PREFIX_FOR_STORAGE_HEADER + "copy-status-description";


            public static final String DATE = PREFIX_FOR_STORAGE_HEADER + "date";


            public static final String DELETE_SNAPSHOT_HEADER = PREFIX_FOR_STORAGE_HEADER + "delete-snapshots";


            public static final String ETAG = "ETag";


            public static final int HTTP_UNUSED_306 = 306;


            public static final String IF_MATCH = "If-Match";


            public static final String IF_MODIFIED_SINCE = "If-Modified-Since";


            public static final String IF_NONE_MATCH = "If-None-Match";


            public static final String IF_UNMODIFIED_SINCE = "If-Unmodified-Since";


            public static final String IF_SEQUENCE_NUMBER_LESS_THAN_OR_EQUAL = PREFIX_FOR_STORAGE_HEADER + "if-sequence-number-le";


            public static final String IF_SEQUENCE_NUMBER_LESS_THAN = PREFIX_FOR_STORAGE_HEADER + "if-sequence-number-lt";


            public static final String IF_SEQUENCE_NUMBER_EQUAL = PREFIX_FOR_STORAGE_HEADER + "if-sequence-number-eq";


            public static final String LEASE_ACTION_HEADER = PREFIX_FOR_STORAGE_HEADER + "lease-action";


            public static final String LEASE_BREAK_PERIOD_HEADER = PREFIX_FOR_STORAGE_HEADER + "lease-break-period";


            public static final String LEASE_DURATION = PREFIX_FOR_STORAGE_HEADER + "lease-duration";


            public static final String LEASE_ID_HEADER = PREFIX_FOR_STORAGE_HEADER + "lease-id";


            public static final String LEASE_STATE = PREFIX_FOR_STORAGE_HEADER + "lease-state";


            public static final String LEASE_STATUS = PREFIX_FOR_STORAGE_HEADER + "lease-status";


            public static final String LEASE_TIME_HEADER = PREFIX_FOR_STORAGE_HEADER + "lease-time";


            public static final String POP_RECEIPT_HEADER = PREFIX_FOR_STORAGE_HEADER + "popreceipt";


            public static final String PREFIX_FOR_STORAGE_METADATA = "x-ms-meta-";


            public static final String PREFIX_FOR_STORAGE_PROPERTIES = "x-ms-prop-";


            public static final String PROPOSED_LEASE_ID_HEADER = PREFIX_FOR_STORAGE_HEADER + "proposed-lease-id";


            public static final String RANGE = "Range";


            public static final String RANGE_GET_CONTENT_MD5 = PREFIX_FOR_STORAGE_HEADER + "range-get-content-md5";


            public static final String RANGE_HEADER_FORMAT = "bytes=%d-%d";


            public static final String REQUEST_ID_HEADER = PREFIX_FOR_STORAGE_HEADER + "request-id";


            public static final String SERVER = "Server";


            public static final String SNAPSHOT_ID_HEADER = PREFIX_FOR_STORAGE_HEADER + "snapshot";


            public static final String SOURCE_IF_MATCH_HEADER = PREFIX_FOR_STORAGE_HEADER + "source-if-match";


            public static final String SOURCE_IF_MODIFIED_SINCE_HEADER = PREFIX_FOR_STORAGE_HEADER
                    + "source-if-modified-since";


            public static final String SOURCE_IF_NONE_MATCH_HEADER = PREFIX_FOR_STORAGE_HEADER + "source-if-none-match";


            public static final String SOURCE_IF_UNMODIFIED_SINCE_HEADER = PREFIX_FOR_STORAGE_HEADER
                    + "source-if-unmodified-since";


            public static final String SOURCE_LEASE_ID_HEADER = PREFIX_FOR_STORAGE_HEADER + "source-lease-id";


            public static final String STORAGE_RANGE_HEADER = PREFIX_FOR_STORAGE_HEADER + "range";


            public static final String STORAGE_VERSION_HEADER = PREFIX_FOR_STORAGE_HEADER + "version";


            public static final String TARGET_STORAGE_VERSION = "2014-02-14";


            public static final String TIME_NEXT_VISIBLE_HEADER = PREFIX_FOR_STORAGE_HEADER + "time-next-visible";


            public static final String USER_AGENT = "User-Agent";


            public static final String USER_AGENT_PREFIX = "Azure-Storage";


            public static final String USER_AGENT_VERSION = "2.2.0";


            public static final String XML_TYPE = "application/xml";
        }


        public static class QueryConstants {


            public static final String CACHE_CONTROL = "rscc";


            public static final String CONTENT_TYPE = "rsct";


            public static final String CONTENT_ENCODING = "rsce";


            public static final String CONTENT_LANGUAGE = "rscl";


            public static final String CONTENT_DISPOSITION = "rscd";


            public static final String COMPONENT = "comp";


            public static final String COPY = "copy";


            public static final String COPY_ID = "copyid";


            public static final String END_PARTITION_KEY = "epk";


            public static final String END_ROW_KEY = "erk";


            public static final String LIST = "list";


            public static final String PROPERTIES = "properties";


            public static final String RESOURCETYPE = "restype";


            public static final String API_VERSION = "api-version";


            public static final String SAS_TABLE_NAME = "tn";


            public static final String SIGNATURE = "sig";


            public static final String SIGNED_EXPIRY = "se";


            public static final String SIGNED_IDENTIFIER = "si";


            public static final String SIGNED_KEY = "sk";


            public static final String SIGNED_PERMISSIONS = "sp";


            public static final String SIGNED_RESOURCE = "sr";


            public static final String SIGNED_START = "st";


            public static final String SIGNED_VERSION = "sv";


            public static final String SNAPSHOT = "snapshot";


            public static final String START_PARTITION_KEY = "spk";


            public static final String START_ROW_KEY = "srk";


            public static final String DELIMITER = "delimiter";


            public static final String INCLUDE = "include";


            public static final String MARKER = "marker";


            public static final String MAX_RESULTS = "maxresults";


            public static final String METADATA = "metadata";


            public static final String PREFIX = "prefix";


            public static final String ACL = "acl";
        }


        public static final String PREFIX_FOR_STORAGE_HEADER = "x-ms-";


        public static final int KB = 1024;


        public static final int MB = 1024 * KB;


        public static final int GB = 1024 * MB;


        public static final String ACCESS_POLICY = "AccessPolicy";


        public static final int BUFFER_COPY_LENGTH = 8 * KB;


        public static final String COPY_COMPLETION_TIME_ELEMENT = "CopyCompletionTime";


        public static final String COPY_ID_ELEMENT = "CopyId";


        public static final String COPY_PROGRESS_ELEMENT = "CopyProgress";


        public static final String COPY_SOURCE_ELEMENT = "CopySource";


        public static final String COPY_STATUS_DESCRIPTION_ELEMENT = "CopyStatusDescription";


        public static final String COPY_STATUS_ELEMENT = "CopyStatus";


        public static final int DEFAULT_READ_TIMEOUT = 5 * 60 * 1000;


        public static final String DELIMITER_ELEMENT = "Delimiter";


        public static final String HTTP_GET = "GET";


        public static final String HTTP_PUT = "PUT";


        public static final String HTTP_DELETE = "DELETE";


        public static final String HTTP_HEAD = "HEAD";


        public static final String HTTP_POST = "POST";


        public static final String EMPTY_STRING = "";


        public static final String END_ELEMENT = "End";


        public static final String ERROR_CODE = "Code";


        public static final String ERROR_EXCEPTION = "ExceptionDetails";


        public static final String ERROR_EXCEPTION_MESSAGE = "ExceptionMessage";


        public static final String ERROR_EXCEPTION_STACK_TRACE = "StackTrace";


        public static final String ERROR_MESSAGE = "Message";


        public static final String ERROR_ROOT_ELEMENT = "Error";


        public static final String ETAG_ELEMENT = "Etag";


        public static final String EXPIRY = "Expiry";


        public static final String FALSE = "false";


        public static final String GEO_BOOTSTRAP_VALUE = "bootstrap";


        public static final String GEO_LIVE_VALUE = "live";


        public static final String GEO_UNAVAILABLE_VALUE = "unavailable";


        public static final String HTTP = "http";


        public static final String HTTPS = "https";


        public static final String ID = "Id";


        public static final String INVALID_METADATA_NAME = "x-ms-invalid-name";


        public static final String LAST_MODIFIED_ELEMENT = "Last-Modified";


        public static final String LEASE_DURATION_ELEMENT = "LeaseDuration";


        public static final String LEASE_STATE_ELEMENT = "LeaseState";


        public static final String LEASE_STATUS_ELEMENT = "LeaseStatus";


        public static final String LOCKED_VALUE = "Locked";


        public static final String MARKER_ELEMENT = "Marker";


        public static int MAX_BLOCK_SIZE = 4 * MB;


        public static final int DEFAULT_STREAM_WRITE_IN_BYTES = Constants.MAX_BLOCK_SIZE;


        public static final int DEFAULT_MINIMUM_READ_SIZE_IN_BYTES = Constants.MAX_BLOCK_SIZE;


        // Note if BlobConstants.MAX_SINGLE_UPLOAD_BLOB_SIZE_IN_BYTES is updated then this needs to be as well.
        public static final int MAX_MARK_LENGTH = 64 * MB;


        public static final String MAX_RESULTS_ELEMENT = "MaxResults";


        public static final int MAX_SHARED_ACCESS_POLICY_IDENTIFIERS = 5;


        public static final int MAXIMUM_SEGMENTED_RESULTS = 5000;


        public static final String METADATA_ELEMENT = "Metadata";


        public static final String NAME_ELEMENT = "Name";


        public static final String NEXT_MARKER_ELEMENT = "NextMarker";


        public static final int PAGE_SIZE = 512;


        public static final String PERMISSION = "Permission";


        public static final String PREFIX_ELEMENT = "Prefix";


        public static final String PROPERTIES = "Properties";


        public static final String SIGNED_IDENTIFIER_ELEMENT = "SignedIdentifier";


        public static final String SIGNED_IDENTIFIERS_ELEMENT = "SignedIdentifiers";


        public static final String START = "Start";


        public static final String TRUE = "true";


        public static final String UNLOCKED_VALUE = "Unlocked";


        public static final String UNSPECIFIED_VALUE = "Unspecified";


        public static final String URL_ELEMENT = "Url";


        public static final String UTF8_CHARSET = "UTF-8";


        private Constants() {
            // No op
        }
    }
}

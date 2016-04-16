package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.annotations.Property;
import org.jgroups.util.Base64;
import org.jgroups.util.Responses;
import org.jgroups.util.Util;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Discovery protocol using Amazon's S3 storage. The S3 access code reuses the example shipped by Amazon.
 * @author Bela Ban
 */
public class S3_PING extends FILE_PING {

    @Property(description="The name of the AWS server")
    protected String host;

    @Property(description="The port at which AWS is listening")
    protected int port;

    @Property(description="Whether or not to use SSL to connect to host:port")
    protected boolean use_ssl=true;

    @Property(description="The access key to AWS (S3)",exposeAsManagedAttribute=false)
    protected String access_key;

    @Property(description="The secret access key to AWS (S3)",exposeAsManagedAttribute=false)
    protected String secret_access_key;

    @Property(description="When non-null, we set location to prefix-UUID")
    protected String prefix;

    @Property(description="When non-null, we use this pre-signed URL for PUTs",exposeAsManagedAttribute=false)
    protected String pre_signed_put_url;

    @Property(description="When non-null, we use this pre-signed URL for DELETEs",exposeAsManagedAttribute=false)
    protected String pre_signed_delete_url;

    @Property(description="Skip the code which checks if a bucket exists in initialization")
    protected boolean skip_bucket_existence_check=false;

    protected AWSAuthConnection conn=null;

    @Override
    public void init() throws Exception {
        super.init();
        if(host == null)
            host=Utils.DEFAULT_HOST;
        validateProperties();
        conn=createConnection();

        if(prefix != null && !prefix.isEmpty()) {
            ListAllMyBucketsResponse bucket_list=conn.listAllMyBuckets(null);
            List buckets=bucket_list.entries;
            if(buckets != null) {
                boolean found=false;
                for(Object tmp: buckets) {
                    if(tmp instanceof Bucket) {
                        Bucket bucket=(Bucket)tmp;
                        if(bucket.name.startsWith(prefix)) {
                            location=bucket.name;
                            found=true;
                        }
                    }
                }
                if(!found) {
                    location=prefix + "-" + java.util.UUID.randomUUID().toString();
                }
            }
        }

        if(usingPreSignedUrls()) {
            PreSignedUrlParser parsedPut = new PreSignedUrlParser(pre_signed_put_url);
            location = parsedPut.getBucket();
        }

        if(!skip_bucket_existence_check && !conn.checkBucketExists(location)) {
            conn.createBucket(location, AWSAuthConnection.LOCATION_DEFAULT, null).connection.getResponseMessage();
        }
    }

    protected AWSAuthConnection createConnection() {
        return port > 0? new AWSAuthConnection(access_key, secret_access_key, use_ssl, host, port)
          : new AWSAuthConnection(access_key, secret_access_key, use_ssl, host);
    }

    @Override
    protected void createRootDir() {
        ; // do *not* create root file system (don't remove !)
    }

    @Override
    protected void readAll(List<Address> members, String clustername, Responses responses) {
        if(clustername == null)
            return;

        try {
            if (usingPreSignedUrls()) {
                PreSignedUrlParser parsedPut = new PreSignedUrlParser(pre_signed_put_url);
                clustername = parsedPut.getPrefix();
            }

            clustername=sanitize(clustername);
            ListBucketResponse rsp=conn.listBucket(location, clustername, null, null, null);
            if(rsp.entries != null) {
                for(Iterator<ListEntry> it=rsp.entries.iterator(); it.hasNext();) {
                    ListEntry key=it.next();
                    try {
                        GetResponse val=conn.get(location, key.key, null);
                        readResponse(val, members, responses);
                    }
                    catch(Throwable t) {
                        log.error("failed reading key %s: %s", key.key, t);
                    }
                }
            }
        }
        catch(IOException ex) {
            log.error(Util.getMessage("FailedReadingAddresses"), ex);
        }
    }


    protected void readResponse(GetResponse rsp, List<Address> mbrs, Responses responses) {
        if(rsp.object == null)
            return;
        byte[] buf=rsp.object.data;
        List<PingData> list;
        if(buf != null && buf.length > 0) {
            try {
                list=read(new ByteArrayInputStream(buf));
                if(list != null) {
                    for(PingData data : list) {
                        if(mbrs == null || mbrs.contains(data.getAddress()))
                            responses.addResponse(data, data.isCoord());
                        if(local_addr != null && !local_addr.equals(data.getAddress()))
                            addDiscoveryResponseToCaches(data.getAddress(), data.getLogicalName(), data.getPhysicalAddr());
                    }
                }
            }
            catch(Throwable e) {
                log.error(Util.getMessage("FailedUnmarshallingResponse"), e);
            }
        }
    }



    @Override
    protected void write(List<PingData> list, String clustername) {
        String filename=addressToFilename(local_addr);
        String key=sanitize(clustername) + "/" + sanitize(filename);
        HttpURLConnection httpConn = null;
        try {
            ByteArrayOutputStream out=new ByteArrayOutputStream(4096);
            write(list, out);
            byte[] data=out.toByteArray();
            S3Object val=new S3Object(data, null);

            if (usingPreSignedUrls()) {
                Map headers = new TreeMap();
                headers.put("x-amz-acl", Arrays.asList("public-read"));
                httpConn = conn.put(pre_signed_put_url, val, headers).connection;
            } else {
                Map headers=new TreeMap();
                headers.put("Content-Type", Arrays.asList("text/plain"));
                httpConn = conn.put(location, key, val, headers).connection;
            }
            if(!httpConn.getResponseMessage().equals("OK")) {
                log.error(Util.getMessage("FailedToWriteFileToS3BucketHTTPResponseCode") + httpConn.getResponseCode() + ")");
            }
        } catch (Exception e) {
            log.error(Util.getMessage("ErrorMarshallingObject"), e);
        }
    }


    protected void remove(String clustername, Address addr) {
        if(clustername == null || addr == null)
            return;
        String filename=addressToFilename(addr);//  addr instanceof org.jgroups.util.UUID? ((org.jgroups.util.UUID)addr).toStringLong() : addr.toString();
        String key=sanitize(clustername) + "/" + sanitize(filename);
        try {
            Map headers=new TreeMap();
            headers.put("Content-Type", Arrays.asList("text/plain"));
            if (usingPreSignedUrls()) {
                conn.delete(pre_signed_delete_url).connection.getResponseMessage();
            } else {
                conn.delete(location, key, headers).connection.getResponseMessage();
            }
            if(log.isTraceEnabled())
                log.trace("removing " + location + "/" + key);
        }
        catch(Exception e) {
            log.error(Util.getMessage("FailureRemovingData"), e);
        }
    }

    @Override
    protected void removeAll(String clustername) {
        if(clustername == null)
            return;

        try {
            Map headers=new TreeMap();
            headers.put("Content-Type", Arrays.asList("text/plain"));
            clustername=sanitize(clustername);
            ListBucketResponse rsp=conn.listBucket(location, clustername, null, null, null);
            if(rsp.entries != null) {
                for(Iterator<ListEntry> it=rsp.entries.iterator(); it.hasNext();) {
                    ListEntry key=it.next();
                    try {
                        if (usingPreSignedUrls())
                            conn.delete(pre_signed_delete_url).connection.getResponseMessage();
                        else
                            conn.delete(location, key.key, headers).connection.getResponseMessage();
                        log.trace("removing %s/%s", location, key.key);
                    }
                    catch(Throwable t) {
                        log.error("failed deleting object %s/%s: %s", location, key.key, t);
                    }
                }
            }
        }
        catch(IOException ex) {
            log.error(Util.getMessage("FailedDeletingAllObjects"), ex);
        }
    }

    protected void validateProperties() {
        if (pre_signed_put_url != null && pre_signed_delete_url != null) {
            PreSignedUrlParser parsedPut = new PreSignedUrlParser(pre_signed_put_url);
            PreSignedUrlParser parsedDelete = new PreSignedUrlParser(pre_signed_delete_url);
            if (!parsedPut.getBucket().equals(parsedDelete.getBucket()) ||
                    !parsedPut.getPrefix().equals(parsedDelete.getPrefix())) {
                throw new IllegalArgumentException("pre_signed_put_url and pre_signed_delete_url must have the same path");
            }
        } else if (pre_signed_put_url != null || pre_signed_delete_url != null) {
            throw new IllegalArgumentException("pre_signed_put_url and pre_signed_delete_url must both be set or both unset");
        }
        if (prefix != null && location != null) {
            throw new IllegalArgumentException("set either prefix or location, but not both");
        }
        if (prefix != null && (access_key == null || secret_access_key == null)) {
            throw new IllegalArgumentException("access_key and secret_access_key must be set when setting prefix");
        }
    }
    
    protected boolean usingPreSignedUrls() {
        return pre_signed_put_url != null;
    }


    /** Sanitizes bucket and folder names according to AWS guidelines */
    protected static String sanitize(final String name) {
        String retval=name;
        retval=retval.replace('/', '-');
        retval=retval.replace('\\', '-');
        return retval;
    }


    /**
     * Use this helper method to generate pre-signed S3 urls for use with S3_PING.
     * You'll need to generate urls for both the put and delete http methods.
     * Example:
     * Your AWS Access Key is "abcd".
     * Your AWS Secret Access Key is "efgh".
     * You want this node to write its information to "/S3_PING/DemoCluster/node1".
     * So, your bucket is "S3_PING" and your key is "DemoCluster/node1".
     * You want this to expire one year from now, or
     *   (System.currentTimeMillis / 1000) + (60 * 60 * 24 * 365)
     *   Let's assume that this equals 1316286684
     * 
     * Here's how to generate the value for the pre_signed_put_url property:
     * String putUrl = S3_PING.generatePreSignedUrl("abcd", "efgh", "put",
     *                                              "S3_Ping", "DemoCluster/node1",
     *                                              1316286684);
     *                                              
     * Here's how to generate the value for the pre_signed_delete_url property:
     * String deleteUrl = S3_PING.generatePreSignedUrl("abcd", "efgh", "delete",
     *                                                 "S3_Ping", "DemoCluster/node1",
     *                                                 1316286684);
     * 
     * @param awsAccessKey Your AWS Access Key
     * @param awsSecretAccessKey Your AWS Secret Access Key
     * @param method The HTTP method - use "put" or "delete" for use with S3_PING
     * @param bucket The S3 bucket you want to write to
     * @param key The key within the bucket to write to
     * @param expirationDate The date this pre-signed url should expire, in seconds since epoch
     * @return The pre-signed url to be used in pre_signed_put_url or pre_signed_delete_url properties
     */
    public static String generatePreSignedUrl(String awsAccessKey, String awsSecretAccessKey, String method,
                                       String bucket, String key, long expirationDate) {
        Map headers = new HashMap();
        if (method.equalsIgnoreCase("PUT")) {
            headers.put("x-amz-acl", Arrays.asList("public-read"));
        }
        return Utils.generateQueryStringAuthentication(awsAccessKey, awsSecretAccessKey, method,
                                                       bucket, key, new HashMap(), headers,
                                                       expirationDate);
    }



    /**
     * Utility class to parse S3 pre-signed URLs
     */
    static class PreSignedUrlParser {
        String bucket = "";
        String prefix = "";

        public PreSignedUrlParser(String preSignedUrl) {
            try {
                URL url = new URL(preSignedUrl);
                this.bucket = parseBucketFromHost(url.getHost());
                String path = url.getPath();
                String[] pathParts = path.split("/");
                
                if (pathParts.length < 2) {
                    throw new IllegalArgumentException("pre-signed url " + preSignedUrl + " must point to a file within a bucket");
                }
                if (pathParts.length > 3) {
                    throw new IllegalArgumentException("pre-signed url " + preSignedUrl + " may only have only subdirectory under a bucket");
                }
                if (pathParts.length > 2) {
                    this.prefix = pathParts[1];
                }
            } catch (MalformedURLException ex) {
                throw new IllegalArgumentException("pre-signed url " + preSignedUrl + " is not a valid url");
            }
        }

        private String parseBucketFromHost(String host) {
            int s3Index = host.lastIndexOf(".s3.");
            if (s3Index > 0) {
                host = host.substring(0, s3Index);
            }
            return host;
        }

        public String getBucket() {
            return bucket;
        }
        
        public String getPrefix() {
            return prefix;
        }
    }

    


    /**
     * The following classes have been copied from Amazon's sample code
     */
    static class AWSAuthConnection {
        public static final String LOCATION_DEFAULT=null;
        public static final String LOCATION_EU="EU";

        private String awsAccessKeyId;
        private String awsSecretAccessKey;
        private boolean isSecure;
        private String server;
        private int port;
        private CallingFormat callingFormat;

        public AWSAuthConnection(String awsAccessKeyId, String awsSecretAccessKey) {
            this(awsAccessKeyId, awsSecretAccessKey, true);
        }

        public AWSAuthConnection(String awsAccessKeyId, String awsSecretAccessKey, boolean isSecure) {
            this(awsAccessKeyId, awsSecretAccessKey, isSecure, Utils.DEFAULT_HOST);
        }

        public AWSAuthConnection(String awsAccessKeyId, String awsSecretAccessKey, boolean isSecure,
                                 String server) {
            this(awsAccessKeyId, awsSecretAccessKey, isSecure, server,
                 isSecure? Utils.SECURE_PORT : Utils.INSECURE_PORT);
        }

        public AWSAuthConnection(String awsAccessKeyId, String awsSecretAccessKey, boolean isSecure,
                                 String server, int port) {
            this(awsAccessKeyId, awsSecretAccessKey, isSecure, server, port, CallingFormat.getSubdomainCallingFormat());

        }

        public AWSAuthConnection(String awsAccessKeyId, String awsSecretAccessKey, boolean isSecure,
                                 String server, CallingFormat format) {
            this(awsAccessKeyId, awsSecretAccessKey, isSecure, server,
                 isSecure? Utils.SECURE_PORT : Utils.INSECURE_PORT,
                 format);
        }

        /**
         * Create a new interface to interact with S3 with the given credential and connection
         * parameters
         * @param awsAccessKeyId     Your user key into AWS
         * @param awsSecretAccessKey The secret string used to generate signatures for authentication.
         * @param isSecure           use SSL encryption
         * @param server             Which host to connect to.  Usually, this will be s3.amazonaws.com
         * @param port               Which port to use.
         * @param format             Type of request Regular/Vanity or Pure Vanity domain
         */
        public AWSAuthConnection(String awsAccessKeyId, String awsSecretAccessKey, boolean isSecure,
                                 String server, int port, CallingFormat format) {
            this.awsAccessKeyId=awsAccessKeyId;
            this.awsSecretAccessKey=awsSecretAccessKey;
            this.isSecure=isSecure;
            this.server=server;
            this.port=port;
            this.callingFormat=format;
        }

        /**
         * Creates a new bucket.
         * @param bucket   The name of the bucket to create.
         * @param headers  A Map of String to List of Strings representing the http headers to pass (can be null).
         */
        public Response createBucket(String bucket, Map headers) throws IOException {
            return createBucket(bucket, null, headers);
        }

        /**
         * Creates a new bucket.
         * @param bucket   The name of the bucket to create.
         * @param location Desired location ("EU") (or null for default).
         * @param headers  A Map of String to List of Strings representing the http
         *                 headers to pass (can be null).
         * @throws IllegalArgumentException on invalid location
         */
        public Response createBucket(String bucket, String location, Map headers) throws IOException {
            String body;
            if(location == null) {
                body=null;
            }
            else if(LOCATION_EU.equals(location)) {
                if(!callingFormat.supportsLocatedBuckets())
                    throw new IllegalArgumentException("Creating location-constrained bucket with unsupported calling-format");
                body="<CreateBucketConstraint><LocationConstraint>" + location + "</LocationConstraint></CreateBucketConstraint>";
            }
            else
                throw new IllegalArgumentException("Invalid Location: " + location);

            // validate bucket name
            if(!Utils.validateBucketName(bucket, callingFormat))
                throw new IllegalArgumentException("Invalid Bucket Name: " + bucket);

            HttpURLConnection request=makeRequest("PUT", bucket, "", null, headers);
            if(body != null) {
                request.setDoOutput(true);
                request.getOutputStream().write(body.getBytes("UTF-8"));
            }
            return new Response(request);
        }

        /**
         * Check if the specified bucket exists (via a HEAD request)
         * @param bucket The name of the bucket to check
         * @return true if HEAD access returned success
         */
        public boolean checkBucketExists(String bucket) throws IOException {
            HttpURLConnection response=makeRequest("HEAD", bucket, "", null, null);
            int httpCode=response.getResponseCode();

            if(httpCode >= 200 && httpCode < 300)
                return true;
            if(httpCode == HttpURLConnection.HTTP_NOT_FOUND) // bucket doesn't exist
                return false;
            throw new IOException("bucket '" + bucket + "' could not be accessed (rsp=" +
                    httpCode + " (" + response.getResponseMessage() + "). Maybe the bucket is owned by somebody else or " +
                    "the authentication failed");

        }

        /**
         * Lists the contents of a bucket.
         * @param bucket  The name of the bucket to create.
         * @param prefix  All returned keys will start with this string (can be null).
         * @param marker  All returned keys will be lexographically greater than
         *                this string (can be null).
         * @param maxKeys The maximum number of keys to return (can be null).
         * @param headers A Map of String to List of Strings representing the http
         *                headers to pass (can be null).
         */
        public ListBucketResponse listBucket(String bucket, String prefix, String marker,
                                             Integer maxKeys, Map headers) throws IOException {
            return listBucket(bucket, prefix, marker, maxKeys, null, headers);
        }

        /**
         * Lists the contents of a bucket.
         * @param bucket    The name of the bucket to list.
         * @param prefix    All returned keys will start with this string (can be null).
         * @param marker    All returned keys will be lexographically greater than
         *                  this string (can be null).
         * @param maxKeys   The maximum number of keys to return (can be null).
         * @param delimiter Keys that contain a string between the prefix and the first
         *                  occurrence of the delimiter will be rolled up into a single element.
         * @param headers   A Map of String to List of Strings representing the http
         *                  headers to pass (can be null).
         */
        public ListBucketResponse listBucket(String bucket, String prefix, String marker,
                                             Integer maxKeys, String delimiter, Map headers) throws IOException {

            Map pathArgs=Utils.paramsForListOptions(prefix, marker, maxKeys, delimiter);
            return new ListBucketResponse(makeRequest("GET", bucket, "", pathArgs, headers));
        }

        /**
         * Deletes a bucket.
         * @param bucket  The name of the bucket to delete.
         * @param headers A Map of String to List of Strings representing the http headers to pass (can be null).
         */
        public Response deleteBucket(String bucket, Map headers) throws IOException {
            return new Response(makeRequest("DELETE", bucket, "", null, headers));
        }

        /**
         * Writes an object to S3.
         * @param bucket  The name of the bucket to which the object will be added.
         * @param key     The name of the key to use.
         * @param object  An S3Object containing the data to write.
         * @param headers A Map of String to List of Strings representing the http
         *                headers to pass (can be null).
         */
        public Response put(String bucket, String key, S3Object object, Map headers) throws IOException {
            HttpURLConnection request=
                    makeRequest("PUT", bucket, Utils.urlencode(key), null, headers, object);

            request.setDoOutput(true);
            request.getOutputStream().write(object.data == null? new byte[]{} : object.data);

            return new Response(request);
        }

        public Response put(String preSignedUrl, S3Object object, Map headers) throws IOException {
            HttpURLConnection request = makePreSignedRequest("PUT", preSignedUrl, headers);
            request.setDoOutput(true);
            request.getOutputStream().write(object.data == null? new byte[]{} : object.data);

            return new Response(request);
        }

        /**
         * Creates a copy of an existing S3 Object.  In this signature, we will copy the
         * existing metadata.  The default access control policy is private; if you want
         * to override it, please use x-amz-acl in the headers.
         * @param sourceBucket      The name of the bucket where the source object lives.
         * @param sourceKey         The name of the key to copy.
         * @param destinationBucket The name of the bucket to which the object will be added.
         * @param destinationKey    The name of the key to use.
         * @param headers           A Map of String to List of Strings representing the http
         *                          headers to pass (can be null).  You may wish to set the x-amz-acl header appropriately.
         */
        public Response copy(String sourceBucket, String sourceKey, String destinationBucket, String destinationKey, Map headers)
                throws IOException {
            S3Object object=new S3Object(new byte[]{}, new HashMap());
            headers=headers == null? new HashMap() : new HashMap(headers);
            headers.put("x-amz-copy-source", Arrays.asList(sourceBucket + "/" + sourceKey));
            headers.put("x-amz-metadata-directive", Arrays.asList("COPY"));
            return verifyCopy(put(destinationBucket, destinationKey, object, headers));
        }

        /**
         * Creates a copy of an existing S3 Object.  In this signature, we will replace the
         * existing metadata.  The default access control policy is private; if you want
         * to override it, please use x-amz-acl in the headers.
         * @param sourceBucket      The name of the bucket where the source object lives.
         * @param sourceKey         The name of the key to copy.
         * @param destinationBucket The name of the bucket to which the object will be added.
         * @param destinationKey    The name of the key to use.
         * @param metadata          A Map of String to List of Strings representing the S3 metadata
         *                          for the new object.
         * @param headers           A Map of String to List of Strings representing the http
         *                          headers to pass (can be null).  You may wish to set the x-amz-acl header appropriately.
         */
        public Response copy(String sourceBucket, String sourceKey, String destinationBucket, String destinationKey, Map metadata, Map headers)
                throws IOException {
            S3Object object=new S3Object(new byte[]{}, metadata);
            headers=headers == null? new HashMap() : new HashMap(headers);
            headers.put("x-amz-copy-source", Arrays.asList(sourceBucket + "/" + sourceKey));
            headers.put("x-amz-metadata-directive", Arrays.asList("REPLACE"));
            return verifyCopy(put(destinationBucket, destinationKey, object, headers));
        }

        /**
         * Copy sometimes returns a successful response and starts to send whitespace
         * characters to us.  This method processes those whitespace characters and
         * will throw an exception if the response is either unknown or an error.
         * @param response Response object from the PUT request.
         * @return The response with the input stream drained.
         * @throws IOException If anything goes wrong.
         */
        private static Response verifyCopy(Response response) throws IOException {
            if(response.connection.getResponseCode() < 400) {
                byte[] body=GetResponse.slurpInputStream(response.connection.getInputStream());
                String message=new String(body);
                if(message.contains("<Error")) {
                    throw new IOException(message.substring(message.indexOf("<Error")));
                }
                else if(message.contains("</CopyObjectResult>")) {
                    // It worked!
                }
                else {
                    throw new IOException("Unexpected response: " + message);
                }
            }
            return response;
        }

        /**
         * Reads an object from S3.
         * @param bucket  The name of the bucket where the object lives.
         * @param key     The name of the key to use.
         * @param headers A Map of String to List of Strings representing the http
         *                headers to pass (can be null).
         */
        public GetResponse get(String bucket, String key, Map headers) throws IOException {
            return new GetResponse(makeRequest("GET", bucket, Utils.urlencode(key), null, headers));
        }

        /**
         * Deletes an object from S3.
         * @param bucket  The name of the bucket where the object lives.
         * @param key     The name of the key to use.
         * @param headers A Map of String to List of Strings representing the http
         *                headers to pass (can be null).
         */
        public Response delete(String bucket, String key, Map headers) throws IOException {
            return new Response(makeRequest("DELETE", bucket, Utils.urlencode(key), null, headers));
        }

        public Response delete(String preSignedUrl) throws IOException {
            return new Response(makePreSignedRequest("DELETE", preSignedUrl, null));
        }

        /**
         * Get the requestPayment xml document for a given bucket
         * @param bucket  The name of the bucket
         * @param headers A Map of String to List of Strings representing the http
         *                headers to pass (can be null).
         */
        public GetResponse getBucketRequestPayment(String bucket, Map headers) throws IOException {
            Map pathArgs=new HashMap();
            pathArgs.put("requestPayment", null);
            return new GetResponse(makeRequest("GET", bucket, "", pathArgs, headers));
        }

        /**
         * Write a new requestPayment xml document for a given bucket
         * @param bucket        The name of the bucket
         * @param requestPaymentXMLDoc
         * @param headers       A Map of String to List of Strings representing the http
         *                      headers to pass (can be null).
         */
        public Response putBucketRequestPayment(String bucket, String requestPaymentXMLDoc, Map headers)
                throws IOException {
            Map pathArgs=new HashMap();
            pathArgs.put("requestPayment", null);
            S3Object object=new S3Object(requestPaymentXMLDoc.getBytes(), null);
            HttpURLConnection request=makeRequest("PUT", bucket, "", pathArgs, headers, object);

            request.setDoOutput(true);
            request.getOutputStream().write(object.data == null? new byte[]{} : object.data);

            return new Response(request);
        }

        /**
         * Get the logging xml document for a given bucket
         * @param bucket  The name of the bucket
         * @param headers A Map of String to List of Strings representing the http headers to pass (can be null).
         */
        public GetResponse getBucketLogging(String bucket, Map headers) throws IOException {
            Map pathArgs=new HashMap();
            pathArgs.put("logging", null);
            return new GetResponse(makeRequest("GET", bucket, "", pathArgs, headers));
        }

        /**
         * Write a new logging xml document for a given bucket
         * @param loggingXMLDoc The xml representation of the logging configuration as a String
         * @param bucket        The name of the bucket
         * @param headers       A Map of String to List of Strings representing the http
         *                      headers to pass (can be null).
         */
        public Response putBucketLogging(String bucket, String loggingXMLDoc, Map headers) throws IOException {
            Map pathArgs=new HashMap();
            pathArgs.put("logging", null);
            S3Object object=new S3Object(loggingXMLDoc.getBytes(), null);
            HttpURLConnection request=makeRequest("PUT", bucket, "", pathArgs, headers, object);

            request.setDoOutput(true);
            request.getOutputStream().write(object.data == null? new byte[]{} : object.data);

            return new Response(request);
        }

        /**
         * Get the ACL for a given bucket
         * @param bucket  The name of the bucket where the object lives.
         * @param headers A Map of String to List of Strings representing the http
         *                headers to pass (can be null).
         */
        public GetResponse getBucketACL(String bucket, Map headers) throws IOException {
            return getACL(bucket, "", headers);
        }

        /**
         * Get the ACL for a given object (or bucket, if key is null).
         * @param bucket  The name of the bucket where the object lives.
         * @param key     The name of the key to use.
         * @param headers A Map of String to List of Strings representing the http
         *                headers to pass (can be null).
         */
        public GetResponse getACL(String bucket, String key, Map headers) throws IOException {
            if(key == null) key="";

            Map pathArgs=new HashMap();
            pathArgs.put("acl", null);

            return new GetResponse(
                    makeRequest("GET", bucket, Utils.urlencode(key), pathArgs, headers)
            );
        }

        /**
         * Write a new ACL for a given bucket
         * @param aclXMLDoc The xml representation of the ACL as a String
         * @param bucket    The name of the bucket where the object lives.
         * @param headers   A Map of String to List of Strings representing the http headers to pass (can be null).
         */
        public Response putBucketACL(String bucket, String aclXMLDoc, Map headers) throws IOException {
            return putACL(bucket, "", aclXMLDoc, headers);
        }

        /**
         * Write a new ACL for a given object
         * @param aclXMLDoc The xml representation of the ACL as a String
         * @param bucket    The name of the bucket where the object lives.
         * @param key       The name of the key to use.
         * @param headers   A Map of String to List of Strings representing the http
         *                  headers to pass (can be null).
         */
        public Response putACL(String bucket, String key, String aclXMLDoc, Map headers)
                throws IOException {
            S3Object object=new S3Object(aclXMLDoc.getBytes(), null);

            Map pathArgs=new HashMap();
            pathArgs.put("acl", null);

            HttpURLConnection request=
                    makeRequest("PUT", bucket, Utils.urlencode(key), pathArgs, headers, object);

            request.setDoOutput(true);
            request.getOutputStream().write(object.data == null? new byte[]{} : object.data);

            return new Response(request);
        }

        public LocationResponse getBucketLocation(String bucket)
                throws IOException {
            Map pathArgs=new HashMap();
            pathArgs.put("location", null);
            return new LocationResponse(makeRequest("GET", bucket, "", pathArgs, null));
        }


        /**
         * List all the buckets created by this account.
         * @param headers A Map of String to List of Strings representing the http
         *                headers to pass (can be null).
         */
        public ListAllMyBucketsResponse listAllMyBuckets(Map headers)
                throws IOException {
            return new ListAllMyBucketsResponse(makeRequest("GET", "", "", null, headers));
        }


        /**
         * Make a new HttpURLConnection without passing an S3Object parameter.
         * Use this method for key operations that do require arguments
         * @param method     The method to invoke
         * @param bucketName the bucket this request is for
         * @param key        the key this request is for
         * @param pathArgs   the
         * @param headers
         * @return
         * @throws MalformedURLException
         * @throws IOException
         */
        private HttpURLConnection makeRequest(String method, String bucketName, String key, Map pathArgs, Map headers)
                throws IOException {
            return makeRequest(method, bucketName, key, pathArgs, headers, null);
        }


        /**
         * Make a new HttpURLConnection.
         * @param method     The HTTP method to use (GET, PUT, DELETE)
         * @param bucket     The bucket name this request affects
         * @param key        The key this request is for
         * @param pathArgs   parameters if any to be sent along this request
         * @param headers    A Map of String to List of Strings representing the http
         *                   headers to pass (can be null).
         * @param object     The S3Object that is to be written (can be null).
         */
        private HttpURLConnection makeRequest(String method, String bucket, String key, Map pathArgs, Map headers,
                                              S3Object object)
                throws IOException {
            CallingFormat format=Utils.getCallingFormatForBucket(this.callingFormat, bucket);
            if(isSecure && format != CallingFormat.getPathCallingFormat() && bucket.contains(".")) {
                System.err.println("You are making an SSL connection, however, the bucket contains periods and the wildcard certificate will not match by default.  Please consider using HTTP.");
            }

            // build the domain based on the calling format
            URL url=format.getURL(isSecure, server, this.port, bucket, key, pathArgs);

            HttpURLConnection connection=(HttpURLConnection)url.openConnection();
            connection.setRequestMethod(method);

            // subdomain-style urls may encounter http redirects.
            // Ensure that redirects are supported.
            if(!connection.getInstanceFollowRedirects()
                    && format.supportsLocatedBuckets())
                throw new RuntimeException("HTTP redirect support required.");

            addHeaders(connection, headers);
            if(object != null) addMetadataHeaders(connection, object.metadata);
            addAuthHeader(connection, method, bucket, key, pathArgs);

            return connection;
        }

        private HttpURLConnection makePreSignedRequest(String method, String preSignedUrl, Map headers) throws IOException {
            URL url = new URL(preSignedUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod(method);

            addHeaders(connection, headers);

            return connection;
        }

        /**
         * Add the given headers to the HttpURLConnection.
         * @param connection The HttpURLConnection to which the headers will be added.
         * @param headers    A Map of String to List of Strings representing the http
         *                   headers to pass (can be null).
         */
        private static void addHeaders(HttpURLConnection connection, Map headers) {
            addHeaders(connection, headers, "");
        }

        /**
         * Add the given metadata fields to the HttpURLConnection.
         * @param connection The HttpURLConnection to which the headers will be added.
         * @param metadata   A Map of String to List of Strings representing the s3
         *                   metadata for this resource.
         */
        private static void addMetadataHeaders(HttpURLConnection connection, Map metadata) {
            addHeaders(connection, metadata, Utils.METADATA_PREFIX);
        }

        /**
         * Add the given headers to the HttpURLConnection with a prefix before the keys.
         * @param connection The HttpURLConnection to which the headers will be added.
         * @param headers    A Map of String to List of Strings representing the http
         *                   headers to pass (can be null).
         * @param prefix     The string to prepend to each key before adding it to the connection.
         */
        private static void addHeaders(HttpURLConnection connection, Map headers, String prefix) {
            if(headers != null) {
                for(Iterator i=headers.keySet().iterator(); i.hasNext();) {
                    String key=(String)i.next();
                    for(Iterator j=((List)headers.get(key)).iterator(); j.hasNext();) {
                        String value=(String)j.next();
                        connection.addRequestProperty(prefix + key, value);
                    }
                }
            }
        }

        /**
         * Add the appropriate Authorization header to the HttpURLConnection.
         * @param connection The HttpURLConnection to which the header will be added.
         * @param method     The HTTP method to use (GET, PUT, DELETE)
         * @param bucket     the bucket name this request is for
         * @param key        the key this request is for
         * @param pathArgs   path arguments which are part of this request
         */
        private void addAuthHeader(HttpURLConnection connection, String method, String bucket, String key, Map pathArgs) {
            if(connection.getRequestProperty("Date") == null) {
                connection.setRequestProperty("Date", httpDate());
            }
            if(connection.getRequestProperty("Content-Type") == null) {
                connection.setRequestProperty("Content-Type", "");
            }

            if(this.awsAccessKeyId != null && this.awsSecretAccessKey != null) {
                String canonicalString=
                        Utils.makeCanonicalString(method, bucket, key, pathArgs, connection.getRequestProperties());
                String encodedCanonical=Utils.encode(this.awsSecretAccessKey, canonicalString, false);
                connection.setRequestProperty("Authorization",
                                              "AWS " + this.awsAccessKeyId + ":" + encodedCanonical);
            }
        }


        /**
         * Generate an rfc822 date for use in the Date HTTP header.
         */
        public static String httpDate() {
            final String DateFormat="EEE, dd MMM yyyy HH:mm:ss ";
            SimpleDateFormat format=new SimpleDateFormat(DateFormat, Locale.US);
            format.setTimeZone(TimeZone.getTimeZone("GMT"));
            return format.format(new Date()) + "GMT";
        }
    }

    static class ListEntry {
        /**
         * The name of the object
         */
        public String key;

        /**
         * The date at which the object was last modified.
         */
        public Date lastModified;

        /**
         * The object's ETag, which can be used for conditional GETs.
         */
        public String eTag;

        /**
         * The size of the object in bytes.
         */
        public long size;

        /**
         * The object's storage class
         */
        public String storageClass;

        /**
         * The object's owner
         */
        public Owner owner;

        public String toString() {
            return key;
        }
    }

    static class Owner {
        public String id;
        public String displayName;
    }


    static class Response {
        public HttpURLConnection connection;

        public Response(HttpURLConnection connection) throws IOException {
            this.connection=connection;
        }
    }


    static class GetResponse extends Response {
        public S3Object object;

        /**
         * Pulls a representation of an S3Object out of the HttpURLConnection response.
         */
        public GetResponse(HttpURLConnection connection) throws IOException {
            super(connection);
            if(connection.getResponseCode() < 400) {
                Map metadata=extractMetadata(connection);
                byte[] body=slurpInputStream(connection.getInputStream());
                this.object=new S3Object(body, metadata);
            }
        }

        /**
         * Examines the response's header fields and returns a Map from String to List of Strings
         * representing the object's metadata.
         */
        private static Map extractMetadata(HttpURLConnection connection) {
            TreeMap metadata=new TreeMap();
            Map headers=connection.getHeaderFields();
            for(Iterator i=headers.keySet().iterator(); i.hasNext();) {
                String key=(String)i.next();
                if(key == null) continue;
                if(key.startsWith(Utils.METADATA_PREFIX)) {
                    metadata.put(key.substring(Utils.METADATA_PREFIX.length()), headers.get(key));
                }
            }

            return metadata;
        }

        /**
         * Read the input stream and dump it all into a big byte array
         */
        static byte[] slurpInputStream(InputStream stream) throws IOException {
            final int chunkSize=2048;
            byte[] buf=new byte[chunkSize];
            ByteArrayOutputStream byteStream=new ByteArrayOutputStream(chunkSize);
            int count;

            while((count=stream.read(buf)) != -1) byteStream.write(buf, 0, count);

            return byteStream.toByteArray();
        }
    }

    static class LocationResponse extends Response {
        String location;

        /**
         * Parse the response to a ?location query.
         */
        public LocationResponse(HttpURLConnection connection) throws IOException {
            super(connection);
            if(connection.getResponseCode() < 400) {
                try {
                    XMLReader xr=Utils.createXMLReader();
                    ;
                    LocationResponseHandler handler=new LocationResponseHandler();
                    xr.setContentHandler(handler);
                    xr.setErrorHandler(handler);

                    xr.parse(new InputSource(connection.getInputStream()));
                    this.location=handler.loc;
                }
                catch(SAXException e) {
                    throw new RuntimeException("Unexpected error parsing ListAllMyBuckets xml", e);
                }
            }
            else {
                this.location="<error>";
            }
        }

        /**
         * Report the location-constraint for a bucket.
         * A value of null indicates an error;
         * the empty string indicates no constraint;
         * and any other value is an actual location constraint value.
         */
        public String getLocation() {
            return location;
        }

        /**
         * Helper class to parse LocationConstraint response XML
         */
        static class LocationResponseHandler extends DefaultHandler {
            String loc=null;
            private StringBuffer currText=null;

            public void startDocument() {
            }

            public void startElement(String uri, String name, String qName, Attributes attrs) {
                if(name.equals("LocationConstraint")) {
                    this.currText=new StringBuffer();
                }
            }

            public void endElement(String uri, String name, String qName) {
                if(name.equals("LocationConstraint")) {
                    loc=this.currText.toString();
                    this.currText=null;
                }
            }

            public void characters(char ch[], int start, int length) {
                if(currText != null)
                    this.currText.append(ch, start, length);
            }
        }
    }


    static class Bucket {
        /**
         * The name of the bucket.
         */
        public String name;

        /**
         * The bucket's creation date.
         */
        public Date creationDate;

        public Bucket() {
            this.name=null;
            this.creationDate=null;
        }

        public Bucket(String name, Date creationDate) {
            this.name=name;
            this.creationDate=creationDate;
        }

        public String toString() {
            return this.name;
        }
    }

    static class ListBucketResponse extends Response {

        /**
         * The name of the bucket being listed.  Null if request fails.
         */
        public String name=null;

        /**
         * The prefix echoed back from the request.  Null if request fails.
         */
        public String prefix=null;

        /**
         * The marker echoed back from the request.  Null if request fails.
         */
        public String marker=null;

        /**
         * The delimiter echoed back from the request.  Null if not specified in
         * the request, or if it fails.
         */
        public String delimiter=null;

        /**
         * The maxKeys echoed back from the request if specified.  0 if request fails.
         */
        public int maxKeys=0;

        /**
         * Indicates if there are more results to the list.  True if the current
         * list results have been truncated.  false if request fails.
         */
        public boolean isTruncated=false;

        /**
         * Indicates what to use as a marker for subsequent list requests in the event
         * that the results are truncated.  Present only when a delimiter is specified.
         * Null if request fails.
         */
        public String nextMarker=null;

        /**
         * A List of ListEntry objects representing the objects in the given bucket.
         * Null if the request fails.
         */
        public List entries=null;

        /**
         * A List of CommonPrefixEntry objects representing the common prefixes of the
         * keys that matched up to the delimiter.  Null if the request fails.
         */
        public List commonPrefixEntries=null;

        public ListBucketResponse(HttpURLConnection connection) throws IOException {
            super(connection);
            if(connection.getResponseCode() < 400) {
                try {
                    XMLReader xr=Utils.createXMLReader();
                    ListBucketHandler handler=new ListBucketHandler();
                    xr.setContentHandler(handler);
                    xr.setErrorHandler(handler);

                    xr.parse(new InputSource(connection.getInputStream()));

                    this.name=handler.getName();
                    this.prefix=handler.getPrefix();
                    this.marker=handler.getMarker();
                    this.delimiter=handler.getDelimiter();
                    this.maxKeys=handler.getMaxKeys();
                    this.isTruncated=handler.getIsTruncated();
                    this.nextMarker=handler.getNextMarker();
                    this.entries=handler.getKeyEntries();
                    this.commonPrefixEntries=handler.getCommonPrefixEntries();

                }
                catch(SAXException e) {
                    throw new RuntimeException("Unexpected error parsing ListBucket xml", e);
                }
            }
        }

        static class ListBucketHandler extends DefaultHandler {

            private String name=null;
            private String prefix=null;
            private String marker=null;
            private String delimiter=null;
            private int maxKeys=0;
            private boolean isTruncated=false;
            private String nextMarker=null;
            private boolean isEchoedPrefix=false;
            private List keyEntries=null;
            private ListEntry keyEntry=null;
            private List commonPrefixEntries=null;
            private CommonPrefixEntry commonPrefixEntry=null;
            private StringBuffer currText=null;
            private SimpleDateFormat iso8601Parser=null;

            public ListBucketHandler() {
                super();
                keyEntries=new ArrayList();
                commonPrefixEntries=new ArrayList();
                this.iso8601Parser=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                this.iso8601Parser.setTimeZone(new SimpleTimeZone(0, "GMT"));
                this.currText=new StringBuffer();
            }

            public void startDocument() {
                this.isEchoedPrefix=true;
            }

            public void endDocument() {
                // ignore
            }

            public void startElement(String uri, String name, String qName, Attributes attrs) {
                if(name.equals("Contents")) {
                    this.keyEntry=new ListEntry();
                }
                else if(name.equals("Owner")) {
                    this.keyEntry.owner=new Owner();
                }
                else if(name.equals("CommonPrefixes")) {
                    this.commonPrefixEntry=new CommonPrefixEntry();
                }
            }

            public void endElement(String uri, String name, String qName) {
                if(name.equals("Name")) {
                    this.name=this.currText.toString();
                }
                // this prefix is the one we echo back from the request
                else if(name.equals("Prefix") && this.isEchoedPrefix) {
                    this.prefix=this.currText.toString();
                    this.isEchoedPrefix=false;
                }
                else if(name.equals("Marker")) {
                    this.marker=this.currText.toString();
                }
                else if(name.equals("MaxKeys")) {
                    this.maxKeys=Integer.parseInt(this.currText.toString());
                }
                else if(name.equals("Delimiter")) {
                    this.delimiter=this.currText.toString();
                }
                else if(name.equals("IsTruncated")) {
                    this.isTruncated=Boolean.valueOf(this.currText.toString());
                }
                else if(name.equals("NextMarker")) {
                    this.nextMarker=this.currText.toString();
                }
                else if(name.equals("Contents")) {
                    this.keyEntries.add(this.keyEntry);
                }
                else if(name.equals("Key")) {
                    this.keyEntry.key=this.currText.toString();
                }
                else if(name.equals("LastModified")) {
                    try {
                        this.keyEntry.lastModified=this.iso8601Parser.parse(this.currText.toString());
                    }
                    catch(ParseException e) {
                        throw new RuntimeException("Unexpected date format in list bucket output", e);
                    }
                }
                else if(name.equals("ETag")) {
                    this.keyEntry.eTag=this.currText.toString();
                }
                else if(name.equals("Size")) {
                    this.keyEntry.size=Long.parseLong(this.currText.toString());
                }
                else if(name.equals("StorageClass")) {
                    this.keyEntry.storageClass=this.currText.toString();
                }
                else if(name.equals("ID")) {
                    this.keyEntry.owner.id=this.currText.toString();
                }
                else if(name.equals("DisplayName")) {
                    this.keyEntry.owner.displayName=this.currText.toString();
                }
                else if(name.equals("CommonPrefixes")) {
                    this.commonPrefixEntries.add(this.commonPrefixEntry);
                }
                // this is the common prefix for keys that match up to the delimiter
                else if(name.equals("Prefix")) {
                    this.commonPrefixEntry.prefix=this.currText.toString();
                }
                if(this.currText.length() != 0)
                    this.currText=new StringBuffer();
            }

            public void characters(char ch[], int start, int length) {
                this.currText.append(ch, start, length);
            }

            public String getName() {
                return this.name;
            }

            public String getPrefix() {
                return this.prefix;
            }

            public String getMarker() {
                return this.marker;
            }

            public String getDelimiter() {
                return this.delimiter;
            }

            public int getMaxKeys() {
                return this.maxKeys;
            }

            public boolean getIsTruncated() {
                return this.isTruncated;
            }

            public String getNextMarker() {
                return this.nextMarker;
            }

            public List getKeyEntries() {
                return this.keyEntries;
            }

            public List getCommonPrefixEntries() {
                return this.commonPrefixEntries;
            }
        }
    }


    static class CommonPrefixEntry {
        /**
         * The prefix common to the delimited keys it represents
         */
        public String prefix;
    }


    static class ListAllMyBucketsResponse extends Response {
        /**
         * A list of Bucket objects, one for each of this account's buckets.  Will be null if
         * the request fails.
         */
        public List entries;

        public ListAllMyBucketsResponse(HttpURLConnection connection) throws IOException {
            super(connection);
            if(connection.getResponseCode() < 400) {
                try {
                    XMLReader xr=Utils.createXMLReader();
                    ;
                    ListAllMyBucketsHandler handler=new ListAllMyBucketsHandler();
                    xr.setContentHandler(handler);
                    xr.setErrorHandler(handler);

                    xr.parse(new InputSource(connection.getInputStream()));
                    this.entries=handler.getEntries();
                }
                catch(SAXException e) {
                    throw new RuntimeException("Unexpected error parsing ListAllMyBuckets xml", e);
                }
            }
        }

        static class ListAllMyBucketsHandler extends DefaultHandler {

            private List entries=null;
            private Bucket currBucket=null;
            private StringBuffer currText=null;
            private SimpleDateFormat iso8601Parser=null;

            public ListAllMyBucketsHandler() {
                super();
                entries=new ArrayList();
                this.iso8601Parser=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                this.iso8601Parser.setTimeZone(new SimpleTimeZone(0, "GMT"));
                this.currText=new StringBuffer();
            }

            public void startDocument() {
                // ignore
            }

            public void endDocument() {
                // ignore
            }

            public void startElement(String uri, String name, String qName, Attributes attrs) {
                if(name.equals("Bucket")) {
                    this.currBucket=new Bucket();
                }
            }

            public void endElement(String uri, String name, String qName) {
                if(name.equals("Bucket")) {
                    this.entries.add(this.currBucket);
                }
                else if(name.equals("Name")) {
                    this.currBucket.name=this.currText.toString();
                }
                else if(name.equals("CreationDate")) {
                    try {
                        this.currBucket.creationDate=this.iso8601Parser.parse(this.currText.toString());
                    }
                    catch(ParseException e) {
                        throw new RuntimeException("Unexpected date format in list bucket output", e);
                    }
                }
                this.currText=new StringBuffer();
            }

            public void characters(char ch[], int start, int length) {
                this.currText.append(ch, start, length);
            }

            public List getEntries() {
                return this.entries;
            }
        }
    }


    static class S3Object {

        public byte[] data;

        /**
         * A Map from String to List of Strings representing the object's metadata
         */
        public Map metadata;

        public S3Object(byte[] data, Map metadata) {
            this.data=data;
            this.metadata=metadata;
        }
    }


    abstract static class CallingFormat {

        protected static CallingFormat pathCallingFormat=new PathCallingFormat();
        protected static CallingFormat subdomainCallingFormat=new SubdomainCallingFormat();
        protected static CallingFormat vanityCallingFormat=new VanityCallingFormat();

        public abstract boolean supportsLocatedBuckets();

        public abstract String getEndpoint(String server, int port, String bucket);

        public abstract String getPathBase(String bucket, String key);

        public abstract URL getURL(boolean isSecure, String server, int port, String bucket, String key, Map pathArgs)
                throws MalformedURLException;

        public static CallingFormat getPathCallingFormat() {
            return pathCallingFormat;
        }

        public static CallingFormat getSubdomainCallingFormat() {
            return subdomainCallingFormat;
        }

        public static CallingFormat getVanityCallingFormat() {
            return vanityCallingFormat;
        }

        private static class PathCallingFormat extends CallingFormat {
            public boolean supportsLocatedBuckets() {
                return false;
            }

            public String getPathBase(String bucket, String key) {
                return isBucketSpecified(bucket)? "/" + bucket + "/" + key : "/";
            }

            public String getEndpoint(String server, int port, String bucket) {
                return server + ":" + port;
            }

            public URL getURL(boolean isSecure, String server, int port, String bucket, String key, Map pathArgs)
                    throws MalformedURLException {
                String pathBase=isBucketSpecified(bucket)? "/" + bucket + "/" + key : "/";
                String pathArguments=Utils.convertPathArgsHashToString(pathArgs);
                return new URL(isSecure? "https" : "http", server, port, pathBase + pathArguments);
            }

            private static boolean isBucketSpecified(String bucket) {
                return bucket != null && bucket.length() != 0;
            }
        }

        private static class SubdomainCallingFormat extends CallingFormat {
            public boolean supportsLocatedBuckets() {
                return true;
            }

            public String getServer(String server, String bucket) {
                return bucket + "." + server;
            }

            public String getEndpoint(String server, int port, String bucket) {
                return getServer(server, bucket) + ":" + port;
            }

            public String getPathBase(String bucket, String key) {
                return "/" + key;
            }

            public URL getURL(boolean isSecure, String server, int port, String bucket, String key, Map pathArgs)
                    throws MalformedURLException {
                if(bucket == null || bucket.length() == 0) {
                    //The bucket is null, this is listAllBuckets request
                    String pathArguments=Utils.convertPathArgsHashToString(pathArgs);
                    return new URL(isSecure? "https" : "http", server, port, "/" + pathArguments);
                }
                else {
                    String serverToUse=getServer(server, bucket);
                    String pathBase=getPathBase(bucket, key);
                    String pathArguments=Utils.convertPathArgsHashToString(pathArgs);
                    return new URL(isSecure? "https" : "http", serverToUse, port, pathBase + pathArguments);
                }
            }
        }

        private static class VanityCallingFormat extends SubdomainCallingFormat {
            public String getServer(String server, String bucket) {
                return bucket;
            }
        }
    }

    static class Utils {
        static final String METADATA_PREFIX="x-amz-meta-";
        static final String AMAZON_HEADER_PREFIX="x-amz-";
        static final String ALTERNATIVE_DATE_HEADER="x-amz-date";
        public static final String DEFAULT_HOST="s3.amazonaws.com";

        public static final int SECURE_PORT=443;
        public static final int INSECURE_PORT=80;


        /**
         * HMAC/SHA1 Algorithm per RFC 2104.
         */
        private static final String HMAC_SHA1_ALGORITHM="HmacSHA1";

        static String makeCanonicalString(String method, String bucket, String key, Map pathArgs, Map headers) {
            return makeCanonicalString(method, bucket, key, pathArgs, headers, null);
        }

        /**
         * Calculate the canonical string.  When expires is non-null, it will be
         * used instead of the Date header.
         */
        static String makeCanonicalString(String method, String bucketName, String key, Map pathArgs,
                                          Map headers, String expires) {
            StringBuilder buf=new StringBuilder();
            buf.append(method + "\n");

            // Add all interesting headers to a list, then sort them.  "Interesting"
            // is defined as Content-MD5, Content-Type, Date, and x-amz-
            SortedMap interestingHeaders=new TreeMap();
            if(headers != null) {
                for(Iterator i=headers.keySet().iterator(); i.hasNext();) {
                    String hashKey=(String)i.next();
                    if(hashKey == null) continue;
                    String lk=hashKey.toLowerCase();

                    // Ignore any headers that are not particularly interesting.
                    if(lk.equals("content-type") || lk.equals("content-md5") || lk.equals("date") ||
                            lk.startsWith(AMAZON_HEADER_PREFIX)) {
                        List s=(List)headers.get(hashKey);
                        interestingHeaders.put(lk, concatenateList(s));
                    }
                }
            }

            if(interestingHeaders.containsKey(ALTERNATIVE_DATE_HEADER)) {
                interestingHeaders.put("date", "");
            }

            // if the expires is non-null, use that for the date field.  this
            // trumps the x-amz-date behavior.
            if(expires != null) {
                interestingHeaders.put("date", expires);
            }

            // these headers require that we still put a new line in after them,
            // even if they don't exist.
            if(!interestingHeaders.containsKey("content-type")) {
                interestingHeaders.put("content-type", "");
            }
            if(!interestingHeaders.containsKey("content-md5")) {
                interestingHeaders.put("content-md5", "");
            }

            // Finally, add all the interesting headers (i.e.: all that startwith x-amz- ;-))
            for(Iterator i=interestingHeaders.keySet().iterator(); i.hasNext();) {
                String headerKey=(String)i.next();
                if(headerKey.startsWith(AMAZON_HEADER_PREFIX)) {
                    buf.append(headerKey).append(':').append(interestingHeaders.get(headerKey));
                }
                else {
                    buf.append(interestingHeaders.get(headerKey));
                }
                buf.append("\n");
            }

            // build the path using the bucket and key
            if(bucketName != null && bucketName.length() != 0) {
                buf.append("/" + bucketName);
            }

            // append the key (it might be an empty string)
            // append a slash regardless
            buf.append("/");
            if(key != null) {
                buf.append(key);
            }

            // if there is an acl, logging or torrent parameter
            // add them to the string
            if(pathArgs != null) {
                if(pathArgs.containsKey("acl")) {
                    buf.append("?acl");
                }
                else if(pathArgs.containsKey("torrent")) {
                    buf.append("?torrent");
                }
                else if(pathArgs.containsKey("logging")) {
                    buf.append("?logging");
                }
                else if(pathArgs.containsKey("location")) {
                    buf.append("?location");
                }
            }

            return buf.toString();

        }

        /**
         * Calculate the HMAC/SHA1 on a string.
         * @return Signature
         * @throws java.security.NoSuchAlgorithmException
         *          If the algorithm does not exist.  Unlikely
         * @throws java.security.InvalidKeyException
         *          If the key is invalid.
         */
        static String encode(String awsSecretAccessKey, String canonicalString,
                             boolean urlencode) {
            // The following HMAC/SHA1 code for the signature is taken from the
            // AWS Platform's implementation of RFC2104 (amazon.webservices.common.Signature)
            //
            // Acquire an HMAC/SHA1 from the raw key bytes.
            SecretKeySpec signingKey=
                    new SecretKeySpec(awsSecretAccessKey.getBytes(), HMAC_SHA1_ALGORITHM);

            // Acquire the MAC instance and initialize with the signing key.
            Mac mac=null;
            try {
                mac=Mac.getInstance(HMAC_SHA1_ALGORITHM);
            }
            catch(NoSuchAlgorithmException e) {
                // should not happen
                throw new RuntimeException("Could not find sha1 algorithm", e);
            }
            try {
                mac.init(signingKey);
            }
            catch(InvalidKeyException e) {
                // also should not happen
                throw new RuntimeException("Could not initialize the MAC algorithm", e);
            }

            // Compute the HMAC on the digest, and set it.
            String b64=Base64.encodeBytes(mac.doFinal(canonicalString.getBytes()));

            if(urlencode) {
                return urlencode(b64);
            }
            else {
                return b64;
            }
        }

        static Map paramsForListOptions(String prefix, String marker, Integer maxKeys) {
            return paramsForListOptions(prefix, marker, maxKeys, null);
        }

        static Map paramsForListOptions(String prefix, String marker, Integer maxKeys, String delimiter) {

            Map argParams=new HashMap();
            // these three params must be url encoded
            if(prefix != null)
                argParams.put("prefix", urlencode(prefix));
            if(marker != null)
                argParams.put("marker", urlencode(marker));
            if(delimiter != null)
                argParams.put("delimiter", urlencode(delimiter));

            if(maxKeys != null)
                argParams.put("max-keys", Integer.toString(maxKeys.intValue()));

            return argParams;

        }

        /**
         * Converts the Path Arguments from a map to String which can be used in url construction
         * @param pathArgs a map of arguments
         * @return a string representation of pathArgs
         */
        public static String convertPathArgsHashToString(Map pathArgs) {
            StringBuilder pathArgsString=new StringBuilder();
            String argumentValue;
            boolean firstRun=true;
            if(pathArgs != null) {
                for(Iterator argumentIterator=pathArgs.keySet().iterator(); argumentIterator.hasNext();) {
                    String argument=(String)argumentIterator.next();
                    if(firstRun) {
                        firstRun=false;
                        pathArgsString.append("?");
                    }
                    else {
                        pathArgsString.append("&");
                    }

                    argumentValue=(String)pathArgs.get(argument);
                    pathArgsString.append(argument);
                    if(argumentValue != null) {
                        pathArgsString.append("=");
                        pathArgsString.append(argumentValue);
                    }
                }
            }

            return pathArgsString.toString();
        }


        static String urlencode(String unencoded) {
            try {
                return URLEncoder.encode(unencoded, "UTF-8");
            }
            catch(UnsupportedEncodingException e) {
                // should never happen
                throw new RuntimeException("Could not url encode to UTF-8", e);
            }
        }

        static XMLReader createXMLReader() {
            try {
                return XMLReaderFactory.createXMLReader();
            }
            catch(SAXException e) {
                // oops, lets try doing this (needed in 1.4)
                System.setProperty("org.xml.sax.driver", "org.apache.crimson.parser.XMLReaderImpl");
            }
            try {
                // try once more
                return XMLReaderFactory.createXMLReader();
            }
            catch(SAXException e) {
                throw new RuntimeException("Couldn't initialize a sax driver for the XMLReader");
            }
        }

        /**
         * Concatenates a bunch of header values, seperating them with a comma.
         * @param values List of header values.
         * @return String of all headers, with commas.
         */
        private static String concatenateList(List values) {
            StringBuilder buf=new StringBuilder();
            for(int i=0, size=values.size(); i < size; ++i) {
                buf.append(((String)values.get(i)).replaceAll("\n", "").trim());
                if(i != (size - 1)) {
                    buf.append(",");
                }
            }
            return buf.toString();
        }

        /**
         * Validate bucket-name
         */
        static boolean validateBucketName(String bucketName, CallingFormat callingFormat) {
            if(callingFormat == CallingFormat.getPathCallingFormat()) {
                final int MIN_BUCKET_LENGTH=3;
                final int MAX_BUCKET_LENGTH=255;
                final String BUCKET_NAME_REGEX="^[0-9A-Za-z\\.\\-_]*$";

                return null != bucketName &&
                        bucketName.length() >= MIN_BUCKET_LENGTH &&
                        bucketName.length() <= MAX_BUCKET_LENGTH &&
                        bucketName.matches(BUCKET_NAME_REGEX);
            }
            else {
                return isValidSubdomainBucketName(bucketName);
            }
        }

        static boolean isValidSubdomainBucketName(String bucketName) {
            final int MIN_BUCKET_LENGTH=3;
            final int MAX_BUCKET_LENGTH=63;
            // don't allow names that look like 127.0.0.1
            final String IPv4_REGEX="^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$";
            // dns sub-name restrictions
            final String BUCKET_NAME_REGEX="^[a-z0-9]([a-z0-9\\-\\_]*[a-z0-9])?(\\.[a-z0-9]([a-z0-9\\-\\_]*[a-z0-9])?)*$";

            // If there wasn't a location-constraint, then the current actual
            // restriction is just that no 'part' of the name (i.e. sequence
            // of characters between any 2 '.'s has to be 63) but the recommendation
            // is to keep the entire bucket name under 63.
            return null != bucketName &&
                    bucketName.length() >= MIN_BUCKET_LENGTH &&
                    bucketName.length() <= MAX_BUCKET_LENGTH &&
                    !bucketName.matches(IPv4_REGEX) &&
                    bucketName.matches(BUCKET_NAME_REGEX);
        }

        static CallingFormat getCallingFormatForBucket(CallingFormat desiredFormat, String bucketName) {
            CallingFormat callingFormat=desiredFormat;
            if(callingFormat == CallingFormat.getSubdomainCallingFormat() && !Utils.isValidSubdomainBucketName(bucketName)) {
                callingFormat=CallingFormat.getPathCallingFormat();
            }
            return callingFormat;
        }

        public static String generateQueryStringAuthentication(String awsAccessKey, String awsSecretAccessKey,
                                                               String method, String bucket, String key,
                                                               Map pathArgs, Map headers) {
            int defaultExpiresIn = 300; // 5 minutes
            long expirationDate = (System.currentTimeMillis() / 1000) + defaultExpiresIn;
            return generateQueryStringAuthentication(awsAccessKey, awsSecretAccessKey,
                                                     method, bucket, key,
                                                     pathArgs, headers, expirationDate);
        }

        public static String generateQueryStringAuthentication(String awsAccessKey, String awsSecretAccessKey,
                                                               String method, String bucket, String key,
                                                               Map pathArgs, Map headers, long expirationDate) {
            method = method.toUpperCase(); // Method should always be uppercase
            String canonicalString =
                makeCanonicalString(method, bucket, key, pathArgs, headers, "" + expirationDate);
            String encodedCanonical = encode(awsSecretAccessKey, canonicalString, true);
            return "https://" + bucket + "." + DEFAULT_HOST + "/" + key + "?" +
                "AWSAccessKeyId=" + awsAccessKey + "&Expires=" + expirationDate +
                "&Signature=" + encodedCanonical;
        }
    }



}






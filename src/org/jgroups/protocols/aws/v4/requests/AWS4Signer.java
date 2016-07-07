package org.jgroups.protocols.aws.v4.requests;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SimpleTimeZone;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Encapsulates signature computation for Amazon S3 requests 
 * using the HTTP Authorization header and Presigned URLs.
 * Calls the following to do the actual computation and signing:
 * AWS4SignerForAuthorizationHeader computes signatures for HTTP Authorization header
 * AWS4SignerForQueryParameterAuth computes signatures for Query string parameters (presigned URLs)
 *  
 * @author Mark Morris
 * 
 */
public class AWS4Signer {
	
    /** SHA256 hash of an empty request body **/
    public static final String EMPTY_BODY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    public static final String UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
    
    public static final String SCHEME = "AWS4";
    public static final String ALGORITHM = "HMAC-SHA256";
    public static final String TERMINATOR = "aws4_request";
    
    /** format strings for the date/time and date stamps required during signing **/
    public static final String ISO8601BasicFormat = "yyyyMMdd'T'HHmmss'Z'";
    public static final String DateStringFormat = "yyyyMMdd";
    
    protected URL endpointUrl;
    protected String httpMethod;
    protected String serviceName;
    protected String regionName;
    
    protected  SimpleDateFormat dateTimeFormat;
    protected  SimpleDateFormat dateStampFormat;
    
    /**
     * Initialize a new AWS V4 signer.
     * 
     * @param endpointUri
     *            The service endpoint, including the path to any resource.
     * @param httpMethod
     *            The HTTP verb for the request, e.g. GET.
     * @param regionName
     *            The system name of the AWS region associated with the
     *            endpoint, e.g. us-east-1.
     */
    public void init( String httpMethod, String regionName, URL endpointUrl  ) 
    {
        this.endpointUrl = endpointUrl;
        this.httpMethod = httpMethod;
        this.serviceName = "s3";
        this.regionName = regionName;
        
        dateTimeFormat = new SimpleDateFormat(ISO8601BasicFormat);
        dateTimeFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
        dateStampFormat = new SimpleDateFormat(DateStringFormat);
        dateStampFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
    }
    
    /**
     * Returns the canonical collection of header names that will be included in
     * the signature. For AWS4, all header names must be included in the process
     * in sorted canonicalized order.
     */
    protected static String getCanonicalizeHeaderNames(Map<String, String> headers) {
        List<String> sortedHeaders = new ArrayList<String>();
        sortedHeaders.addAll(headers.keySet());
        Collections.sort(sortedHeaders, String.CASE_INSENSITIVE_ORDER);

        StringBuilder buffer = new StringBuilder();
        for (String header : sortedHeaders) {
            if (buffer.length() > 0) buffer.append(";");
            buffer.append(header.toLowerCase());
        }

        return buffer.toString();
    }
    
    /**
     * Computes the canonical headers with values for the request. For AWS4, all
     * headers must be included in the signing process.
     */
    protected static String getCanonicalizedHeaderString(Map<String, String> headers) {
        if ( headers == null || headers.isEmpty() ) {
            return "";
        }
        
        // step1: sort the headers by case-insensitive order
        List<String> sortedHeaders = new ArrayList<String>();
        sortedHeaders.addAll(headers.keySet());
        Collections.sort(sortedHeaders, String.CASE_INSENSITIVE_ORDER);

        // step2: form the canonical header:value entries in sorted order. 
        // Multiple white spaces in the values should be compressed to a single 
        // space.
        StringBuilder buffer = new StringBuilder();
        for (String key : sortedHeaders) {
            buffer.append(key.toLowerCase().replaceAll("\\s+", " ") + ":" + headers.get(key).replaceAll("\\s+", " "));
            buffer.append("\n");
        }

        return buffer.toString();
    }
    
    /**
     * Returns the canonical request string to go into the signer process; this 
       consists of several canonical sub-parts.
     * @return
     */
    protected static String getCanonicalRequest(URL endpoint, 
                                         String httpMethod,
                                         String queryParameters, 
                                         String canonicalizedHeaderNames,
                                         String canonicalizedHeaders, 
                                         String bodyHash) {
        String canonicalRequest =
                        httpMethod + "\n" +
                        getCanonicalizedResourcePath(endpoint) + "\n" +
                        queryParameters + "\n" +
                        canonicalizedHeaders + "\n" +
                        canonicalizedHeaderNames + "\n" +
                        bodyHash;
        return canonicalRequest;
    }
    
    /**
     * Returns the canonicalized resource path for the service endpoint.
     */
    protected static String getCanonicalizedResourcePath(URL endpoint) {
        if ( endpoint == null ) {
            return "/";
        }
        String path = endpoint.getPath();
        if ( path == null || path.isEmpty() ) {
            return "/";
        }
        
        String encodedPath = urlEncode(path, true);
        if (encodedPath.startsWith("/")) {
            return encodedPath;
        } else {
            return "/".concat(encodedPath);
        }
    }
    
    /**
     * Examines the specified query string parameters and returns a
     * canonicalized form.
     * <p>
     * The canonicalized query string is formed by first sorting all the query
     * string parameters, then URI encoding both the key and value and then
     * joining them, in order, separating key value pairs with an '&'.
     *
     * @param parameters
     *            The query string parameters to be canonicalized.
     *
     * @return A canonicalized form for the specified query string parameters.
     */
    public static String getCanonicalizedQueryString(Map<String, String> parameters) {
        if ( parameters == null || parameters.isEmpty() ) {
            return "";
        }
        
        SortedMap<String, String> sorted = new TreeMap<String, String>();

        Iterator<Map.Entry<String, String>> pairs = parameters.entrySet().iterator();
        while (pairs.hasNext()) {
            Map.Entry<String, String> pair = pairs.next();
            String key = pair.getKey();
            String value = pair.getValue();
            sorted.put(urlEncode(key, false), urlEncode(value, false));
        }

        StringBuilder builder = new StringBuilder();
        pairs = sorted.entrySet().iterator();
        while (pairs.hasNext()) {
            Map.Entry<String, String> pair = pairs.next();
            builder.append(pair.getKey());
            if(pair.getValue().length() >= 0)
            {
            	builder.append("=");
            	builder.append(pair.getValue());
            }
            if (pairs.hasNext()) {
                builder.append("&");
            }
        }

        return builder.toString();
    }
    
    protected static String getStringToSign(String scheme, String algorithm, String dateTime, String scope, String canonicalRequest) {
        String stringToSign =
                        scheme + "-" + algorithm + "\n" +
                        dateTime + "\n" +
                        scope + "\n" +
                        toHex(hash(canonicalRequest));
        return stringToSign;
    }
    
    /**
     * Hashes the string contents (assumed to be UTF-8) using the SHA-256
     * algorithm.
     */
    public static byte[] hash(String text) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(text.getBytes("UTF-8"));
            return md.digest();
        } catch (Exception e) {
            throw new RuntimeException("Unable to compute hash while signing request: " + e.getMessage(), e);
        }
    }
    
    /**
     * Hashes the byte array using the SHA-256 algorithm.
     */
    public static byte[] hash(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(data);
            return md.digest();
        } catch (Exception e) {
            throw new RuntimeException("Unable to compute hash while signing request: " + e.getMessage(), e);
        }
    }
    
    protected static byte[] sign(String stringData, byte[] key, String algorithm) {
        try {
            byte[] data = stringData.getBytes("UTF-8");
            Mac mac = Mac.getInstance(algorithm);
            mac.init(new SecretKeySpec(key, algorithm));
            return mac.doFinal(data);
        } catch (Exception e) {
            throw new RuntimeException("Unable to calculate a request signature: " + e.getMessage(), e);
        }
    }

    /////////////////////////////////////////////////////////////
    
    protected static AWS4Signer awsSignerForAuthorizationHeader = new AWS4SignerForAuthorizationHeader();
    protected static AWS4Signer awsSignerForQueryParameterAuth  = new AWS4SignerForQueryParameterAuth();
     
    public static AWS4Signer getAWS4SignerForAuthorizationHeader() {
        return awsSignerForAuthorizationHeader;
    }
   
    public static AWS4Signer getAWS4SignerForQueryParameterAuth() {
        return awsSignerForQueryParameterAuth;
    }
   
   ////////////////////////////////////////////////////////////////
    
    public static String toHex(byte[] data) {
        StringBuilder sb = new StringBuilder(data.length * 2);
        for (int i = 0; i < data.length; i++) {
            String hex = Integer.toHexString(data[i]);
            if (hex.length() == 1) {
                // Append leading zero.
                sb.append("0");
            } else if (hex.length() == 8) {
                // Remove ff prefix from negative numbers.
                hex = hex.substring(6);
            }
            sb.append(hex);
        }
        return sb.toString().toLowerCase(Locale.getDefault());
    }

    public static byte[] fromHex(String hexData) {
        byte[] result = new byte[(hexData.length() + 1) / 2];
        String hexNumber = null;
        int stringOffset = 0;
        int byteOffset = 0;
        while (stringOffset < hexData.length()) {
            hexNumber = hexData.substring(stringOffset, stringOffset + 2);
            stringOffset += 2;
            result[byteOffset++] = (byte) Integer.parseInt(hexNumber, 16);
        }
        return result;
    }

    ////////////////////////////////////////////////////////////////////////////

    public static HttpURLConnection getHttpRequest(URL endpointUrl,
            String httpMethod,
            Map<String, String> headers,
            String requestBody) {

    	httpMethod = httpMethod.toUpperCase();

    	HttpURLConnection connection = createHttpConnection(endpointUrl, httpMethod, headers);

    	return connection;
    }

   
    public static String invokeHttpRequest(URL endpointUrl,
                                         String httpMethod,
                                         Map<String, String> headers,
                                         String requestBody) {
    	
    	httpMethod = httpMethod.toUpperCase();
    	
        HttpURLConnection connection = createHttpConnection(endpointUrl, httpMethod, headers);
        
        try {
            if ( requestBody != null ) {
                DataOutputStream wr = new DataOutputStream(
                        connection.getOutputStream());
                wr.writeBytes(requestBody);
                wr.flush();
                wr.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("Request failed. " + e.getMessage(), e);
        }
        
        return executeHttpRequest(connection);
    }
    
    private static String executeHttpRequest(HttpURLConnection connection) {
        try {
        	
            // Get Response
            InputStream is;
            try {
                is = connection.getInputStream();
            } catch (IOException e) {
                is = connection.getErrorStream();
            }
            
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = rd.readLine()) != null) {
                response.append(line);
                response.append('\r');
            }
            rd.close();
            return response.toString();
        } catch (Exception e) {
            throw new RuntimeException("Request failed. " + e.getMessage(), e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
    
    public static HttpURLConnection createHttpConnection(URL endpointUrl,
                                                         String httpMethod,
                                                         Map<String, String> headers) {
        try {
            HttpURLConnection connection = (HttpURLConnection) endpointUrl.openConnection();
            connection.setRequestMethod(httpMethod);
            
            if ( headers != null ) {
                System.out.println("--------- Request headers ---------");
                for ( String headerKey : headers.keySet() ) {
                    System.out.println(headerKey + ": " + headers.get(headerKey));
                    connection.setRequestProperty(headerKey, headers.get(headerKey));
                }
            }

            connection.setUseCaches(false);
            connection.setDoInput(true);
            connection.setDoOutput(true);
            return connection;
        } catch (Exception e) {
            throw new RuntimeException("Cannot create connection. " + e.getMessage(), e);
        }
    }
    
    private static String urlEncode(String url, boolean keepPathSlash) {
        String encoded;
        try {
            encoded = URLEncoder.encode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 encoding is not supported.", e);
        }
        if ( keepPathSlash ) {
            encoded = encoded.replace("%2F", "/");
        }
        return encoded;
    }
    
    
    ///////////////////////////////////////////////////////////////////////////////
    public static String getAWS4AuthorizationForHeader(
   		 	boolean isSecure
   			, String method
			, String bucketName // must be valid, we don't check!
			, String regionName
    		, Map<String, String> headers
    		, Map<String, String> queryParameters
    		, String bodyHash
    		, String awsAccessKey
    		, String awsSecretKey) 
    {
        String authorization = null;    	
        String scheme = "https://"; // default is ssl          
  	
        try {
        		AWS4SignerForAuthorizationHeader signer = 
        				(AWS4SignerForAuthorizationHeader)AWS4Signer.getAWS4SignerForAuthorizationHeader();
       			
		    	if(!isSecure){
		    		scheme = "http://"; // set scheme non-ssl
		    	}
		    	URL endpointUrl = new URL(scheme + bucketName + ".s3.amazonaws.com/");
                
       			method = method.toUpperCase();
        		signer.init(method, regionName, endpointUrl);
        		
        		authorization = signer.computeSignature(
        				headers, queryParameters, bodyHash, awsAccessKey, awsSecretKey);
        
        } catch (Exception e) {
            throw new RuntimeException( "Exception caught in getAWS4AuthorizationForHeader: " + e.getMessage() );
        }
        		
        return authorization;

    }
  
    public static String getAWS4AuthorizationForHeader(
    		URL endpointUrl
   			, String method
			, String bucketName // must be valid, we don't check!
			, String regionName
    		, Map<String, String> headers
    		, Map<String, String> queryParameters
    		, String bodyHash
    		, String awsAccessKey
    		, String awsSecretKey) 
    {
        String authorization = null;    	
        String scheme = "https://"; // default is ssl          
  	
        try {
        		AWS4SignerForAuthorizationHeader signer = 
        				(AWS4SignerForAuthorizationHeader)AWS4Signer.getAWS4SignerForAuthorizationHeader();
       			                       			
        		signer.init(method.toUpperCase(), regionName, endpointUrl);
        		
        		authorization = signer.computeSignature(
        				headers, queryParameters, bodyHash, awsAccessKey, awsSecretKey);
        
        } catch (Exception e) {
            throw new RuntimeException( "Exception caught in getAWS4AuthorizationForHeader: " + e.getMessage() );
        }
        		
        return authorization;

    }

    public static String getAWS4AuthorizationForHeader(
   		 	boolean isSecure
   			, String method
    		, Map<String, String> headers
    		, Map<String, String> queryParameters
    		, String bodyHash
    		, String awsAccessKey
    		, String awsSecretKey) 
    {
        String authorization = null;    	
        String scheme = "https://"; // default is ssl          
  	
        try {
        		AWS4SignerForAuthorizationHeader signer = 
        				(AWS4SignerForAuthorizationHeader)AWS4Signer.getAWS4SignerForAuthorizationHeader();
       			
		    	if(!isSecure){
		    		scheme = "http://"; // set scheme non-ssl
		    	}
		    	URL endpointUrl = new URL(scheme + "s3.amazonaws.com/");
                
       			method = method.toUpperCase();
        		signer.init(method, "us-east-1", endpointUrl);
        		
        		authorization = signer.computeSignature(
        				headers, queryParameters, bodyHash, awsAccessKey, awsSecretKey);
        
        } catch (Exception e) {
            throw new RuntimeException( "Exception caught in getAWS4AuthorizationForHeader: " + e.getMessage() );
        }
        		
        return authorization;

    }

    
    public static String getAWS4AuthorizationForHeader(
    		URL endpointUrl
   			, String method
			, String regionName
    		, Map<String, String> headers
    		, Map<String, String> queryParameters
    		, String bodyHash
    		, String awsAccessKey
    		, String awsSecretKey) 
    {
        String authorization = null;    	
  	
        try {
        		AWS4SignerForAuthorizationHeader signer = 
        				(AWS4SignerForAuthorizationHeader)AWS4Signer.getAWS4SignerForAuthorizationHeader();
        		
       			method = method.toUpperCase();
       			
        		signer.init(method, regionName, endpointUrl);
        		
        		authorization = signer.computeSignature(
        				headers, queryParameters, bodyHash, awsAccessKey, awsSecretKey);
        
        } catch (Exception e) {
            throw new RuntimeException( "Exception caught in getAWS4AuthorizationForHeader: " + e.getMessage() );
        }
        		
        return authorization;

    }

    
    public static String getAWS4AuthorizationForHeader(
   		 	boolean isSecure
   			, String method
			, String bucketName // must be valid, we don't check!
			, String key
			, String regionName
    		, Map<String, String> headers
    		, Map<String, String> queryParameters
    		, String bodyHash
    		, String awsAccessKey
    		, String awsSecretKey) 
    {
        String authorization = null;    	
  	
        try {
        		AWS4SignerForAuthorizationHeader signer = 
        				(AWS4SignerForAuthorizationHeader)AWS4Signer.getAWS4SignerForAuthorizationHeader();
        		
       			URL endpointUrl = buildEndpointURLForVirtualHostedStyle(isSecure, regionName, bucketName, key);
                
       			method = method.toUpperCase();
        		signer.init(method, regionName, endpointUrl);
        		
        		authorization = signer.computeSignature(
        				headers, queryParameters, bodyHash, awsAccessKey, awsSecretKey);
        
        } catch (Exception e) {
            throw new RuntimeException( "Exception caught in getAWS4AuthorizationForHeader: " + e.getMessage() );
        }
        		
        return authorization;

    }
    
    public static String getAWS4AuthorizationForQueryParameterAuth(
   		 	boolean isSecure
   			, String method
			, String bucketName // must be valid, we don't check!
			, String key
			, String regionName
    		, Map<String, String> headers
    		, Map<String, String> queryParameters
    		, String bodyHash
    		, String awsAccessKey
    		, String awsSecretKey) 
        {
            String authorization = null;    	
    	
            try {
            	
           			AWS4SignerForQueryParameterAuth signer = 
            				(AWS4SignerForQueryParameterAuth)AWS4Signer.getAWS4SignerForQueryParameterAuth();
            		
           			URL endpointUrl = buildEndpointURLForPathStyle(isSecure, regionName, bucketName, key);
           			
           			method = method.toUpperCase();
            		signer.init(method, regionName, endpointUrl);
            		
            		authorization = signer.computeSignature(
            				headers, queryParameters, bodyHash, awsAccessKey, awsSecretKey);
            
            } catch (Exception e) {
                throw new RuntimeException( "Exception caught in getAWS4AuthorizationForQueryParameterAuth: " + e.getMessage() );
            }
            		
            return authorization;

        }
        
    ///////////////////////////////////////////////////////////////////////////////

    public static String getPresignedURL(
			String method
			, boolean isSecure
			, String bucketName // must be valid, we don't check!
			, String regionName
			, String key
			, Map<String, String> queryParameters
			, Map<String, String> headers
			, String bodyHash
			, String awsAccessKey
			, String awsSecretKey
			) 
	{
        
         method = method.toUpperCase();
         
        //////////////////////////////////////////////////////////////////
        
        String authorization = getAWS4AuthorizationForQueryParameterAuth(
       		 	isSecure
       			, method
    			, bucketName 
    			, key
        		, regionName
        		, headers
        		, queryParameters
         		, bodyHash
         		, awsAccessKey
        		, awsSecretKey
        		);
                
        // add authorization header
        headers.put( "Authorization", authorization );
        
        //////////////////////////////////////////////////////////////
      
        String scheme = "https://";
        
        URL endpointUrl = buildEndpointURLForPathStyle(isSecure, regionName, bucketName, key);

        
        String presignedUrl = endpointUrl.toString() + "?" + authorization;
		
		return presignedUrl;
		
	}
	    	
    public static String getObject(
     		Boolean isSecure
    		, String regionName
     		, String bucketName
    		, String key
    		, String awsAccessKey
    		, String awsSecretKey
    		) 
    {
        String method = "GET";
        
        // supply the pre-computed 'empty' hash
        Map<String, String> headers = new HashMap<String, String>();
        headers.put( "x-amz-content-sha256", AWS4Signer.EMPTY_BODY_SHA256 );
        
        // no query parameters
        Map<String, String> queryParameters = null;
        
        // empty body hash
        String bodyHash = AWS4Signer.EMPTY_BODY_SHA256;
        
        
        //////////////////////////////////////////////////////////////////
                
        String authorization = getAWS4AuthorizationForHeader(
       		 	isSecure
       			, method
    			, bucketName 
    			, key
        		, regionName
        		, headers
        		, queryParameters
         		, bodyHash
         		, awsAccessKey
        		, awsSecretKey
        		);
                
        // add authorization header
        headers.put( "Authorization", authorization );
        
        //////////////////////////////////////////////////////////////
        
       URL endpointUrl = buildEndpointURLForVirtualHostedStyle(isSecure, regionName, bucketName, key);

        // make HTTP request, null request body
        String response = invokeHttpRequest( endpointUrl, method, headers, null );
        
        return response;
        
    }

    public static String putObject(
     		Boolean isSecure
     		, String bucketName
    		, String regionName
     		, String key
    		, String awsAccessKey
    		, String awsSecretKey
    		, String objectContent
    		) 
    {

        String method = "PUT";
        
        // precompute hash of the body content
        byte[] contentHash = hash(objectContent);
        String bodyHash = toHex(contentHash);
        
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("x-amz-content-sha256", bodyHash);
        int len = objectContent.length();
        String content_length = "" + len;
        headers.put("content-length", content_length);
        headers.put("x-amz-storage-class", "REDUCED_REDUNDANCY");
        
        Map<String, String> queryParameters = null; // no query parameters
        
        String authorization = getAWS4AuthorizationForHeader(
       		 	isSecure
       			, method
    			, bucketName 
    			, key
        		, regionName
        		, headers
        		, queryParameters
         		, bodyHash
         		, awsAccessKey
        		, awsSecretKey
        		);
                
        // add authorization header
        headers.put( "Authorization", authorization );
        
        //////////////////////////////////////////////////////////////
                    
        URL endpointUrl = buildEndpointURLForVirtualHostedStyle(isSecure, regionName, bucketName, key);
        
        // make the call to Amazon S3
        String response = invokeHttpRequest(endpointUrl, method, headers, objectContent);
        
        return response;

    }
               
	public static URL buildEndpointURLForPathStyle(Boolean isSecure, String regionName, String bucketName, String key) {

		URL endpointUrl; // we build it
        String scheme = "https://"; // default
		   
		   try {
			   
		    	if(!isSecure){
		    		scheme = "http://"; // set scheme non-ssl
		    	}
		    	
		        if (regionName == null || regionName.equals("us-east-1")) 
		        {
		            endpointUrl = new URL(scheme + "s3.amazonaws.com/" + bucketName + "/" + key);
		            
		        } else {
		        	
		            endpointUrl = new URL(scheme + "s3-" + regionName + ".amazonaws.com/" + bucketName + "/" + key);
		        }
		        
		    } catch (Exception e) {
		        throw new RuntimeException("Exception caught: " + e.getMessage());
		    }
		return endpointUrl;
	}
       
	public static URL buildEndpointURLForVirtualHostedStyle(Boolean isSecure, String regionName, String bucketName, String key) {
		
		URL endpointUrl; // we build it
        String scheme = "https://"; // default
		   
		   try {
			   
		    	if(!isSecure){
		    		scheme = "http://"; // set scheme non-ssl
		    	}
		    	
		        if (regionName == null || regionName.equals("us-east-1")) 
		        {
		            endpointUrl = new URL(scheme + bucketName + "." + "s3.amazonaws.com/" + key);
		            
		        } else {
		        	
		            endpointUrl = new URL(scheme + bucketName + "." + "s3-" + regionName + ".amazonaws.com/" + key);
		        }
		        
		    } catch (Exception e) {
		        throw new RuntimeException("Exception caught: " + e.getMessage());
		    }
		return endpointUrl;
	}
                  
	public static URL buildEndpointURLForCNAME(Boolean isSecure, String bucketName, String key) {
		
		URL endpointUrl; // we build it
        String scheme = "https://"; // default
		   
		   try {
			   
		    	if(!isSecure){
		    		scheme = "http://"; // set scheme non-ssl
		    	}
		            
		    	endpointUrl = new URL(scheme + bucketName + "/" + key);
		        
		    } catch (Exception e) {
		        throw new RuntimeException("Exception caught: " + e.getMessage());
		    }
		return endpointUrl;
	}
       
       
}


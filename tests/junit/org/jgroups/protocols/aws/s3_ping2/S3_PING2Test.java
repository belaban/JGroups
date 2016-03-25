package org.jgroups.protocols.aws.s3_ping2;

import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jgroups.Global;
import org.jgroups.protocols.aws.s3_ping2.S3_PING2;
import org.jgroups.protocols.aws.s3_ping2.S3_PING2.AWSAuthConnection;
import org.jgroups.protocols.aws.s3_ping2.S3_PING2.Bucket;
import org.jgroups.protocols.aws.s3_ping2.S3_PING2.GetResponse;
import org.jgroups.protocols.aws.s3_ping2.S3_PING2.ListAllMyBucketsResponse;
import org.jgroups.protocols.aws.s3_ping2.S3_PING2.ListBucketResponse;
import org.jgroups.protocols.aws.s3_ping2.S3_PING2.LocationResponse;
import org.jgroups.protocols.aws.s3_ping2.S3_PING2.PreSignedUrlParser;
import org.jgroups.protocols.aws.s3_ping2.S3_PING2.Response;
import org.jgroups.protocols.aws.s3_ping2.S3_PING2.S3Object;
import org.jgroups.protocols.aws.s3_ping2.S3_PING2.Utils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups={Global.STACK_INDEPENDENT})
public class S3_PING2Test {
	
	//////////////////////////////////////////////////////////////////////////
    
	private S3_PING2 s3_ping2 = null;
    
    private AWSAuthConnection awsConn = null;
    
    //private String bucketName = System.getenv("bucketName");
	private String bucketName = "eu-central-1-tti-alice";	
	//private String bucketName = "eu-central-1-1452380393802";	
	
	//private String regionName = System.getenv("regionName");
	private String regionName = "eu-central-1"; // Frankfurt eu-central-1;
	
	//private String key = System.getenv("key");
	private String folder = "notes/"; // case sensitive
	private String key = "notes/MARK.txt"; // case sensitive
	private String objectContent = "Test " + new Date().toString();
	
	private boolean isSecure = true;
	
	private String prefix = null;
	
	private String pre_signed_put_url = null;
	private String pre_signed_delete_url = null;
	
	private boolean bCreateBucket = false;
	private boolean bBucketCreated = false;
	
	//////////////////////////////////////////////////////////////////////////

	
	
    @BeforeMethod
    void setUp() {
        s3_ping2 = new S3_PING2();
    	s3_ping2.region = regionName;
    	s3_ping2.host = Utils.DEFAULT_HOST;
    	s3_ping2.port = 0;
    	s3_ping2.use_ssl = isSecure;
    	s3_ping2.access_key = System.getenv("awsAccessKey");
    	s3_ping2.secret_access_key = System.getenv("awsSecretKey");
    	s3_ping2.skip_bucket_existence_check = true;
    	s3_ping2.prefix = prefix;
    	s3_ping2.pre_signed_put_url = pre_signed_put_url;
    	s3_ping2.pre_signed_delete_url = pre_signed_delete_url;
     	awsConn = s3_ping2.createConnection();
    }
        
    ////////////////////////////////////////////////////////////////
    
    @Test(dependsOnMethods = { "testCheckBucketExists" })
    public void testPutBucketRequestPayment() {
    	
        try {
        	Assert.assertNotNull(awsConn);
        	///////////////////////////////////////////////
        	// PutBucketRequestPayment
        	///////////////////////////////////////////////
        	// owner pays
        	s3_ping2.s3_ping2_log("PutBucketRequestPayment");
        	String msg = "<RequestPaymentConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Payer>BucketOwner</Payer></RequestPaymentConfiguration>";
        	HttpURLConnection httpConn = awsConn.putBucketRequestPayment(bucketName, msg, null).connection;
        	String rspmsg = httpConn.getResponseMessage();
        	int code = httpConn.getResponseCode();
        	String response = "" + code + " " + rspmsg;
        	s3_ping2.s3_ping2_log("PutBucketRequestPayment: " + response);

        } catch (Exception e) {
			e.printStackTrace();
		}
        
    }

    @Test(dependsOnMethods = { "testCheckBucketExists" })
    public void testPutBucketLogging() {
    	
        try {
        	Assert.assertNotNull(awsConn);
        	///////////////////////////////////////////////
        	// PutBucketLogging
        	///////////////////////////////////////////////
        	// disable logging
        	s3_ping2.s3_ping2_log("PutBucketLogging");
        	String msg = "<BucketLoggingStatus xmlns=\"http://doc.s3.amazonaws.com/2006-03-01\" />";
        	HttpURLConnection httpConn = awsConn.putBucketLogging(bucketName, msg, null).connection;
        	String rspmsg = httpConn.getResponseMessage();
        	int code = httpConn.getResponseCode();
        	String response = "" + code + " " + rspmsg;
        	s3_ping2.s3_ping2_log("PutBucketLogging: " + response);

        } catch (Exception e) {
			e.printStackTrace();
		}
        
    }

    @Test(dependsOnMethods = { "testCheckBucketExists" })
    public void testPutAcl() {
    	
        try {
        	Assert.assertNotNull(awsConn);
        	///////////////////////////////////////////////
        	// PutAcl
        	///////////////////////////////////////////////
        	s3_ping2.s3_ping2_log("PutAcl");
        	String msg = "<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Owner><ID>" + System.getenv("ID") + "</ID></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>d36e59ad2a2e2f1e1cfdd2efd83e5a50bd1cf90c76e9bc09045d8f19405a495a</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>";
        	HttpURLConnection httpConn = awsConn.putACL(bucketName, key, msg, null).connection;
        	String rspmsg = httpConn.getResponseMessage();
        	int code = httpConn.getResponseCode();
        	String response = "" + code + " " + rspmsg;
        	s3_ping2.s3_ping2_log("PutAcl: " + response);

        } catch (Exception e) {
			e.printStackTrace();
		}
        
    }

    @Test(dependsOnMethods = { "testCheckBucketExists" })
    public void testPutObject() {
    	
        try {
        	Assert.assertNotNull(awsConn);
        	///////////////////////////////////////////////
        	// PutObject
        	///////////////////////////////////////////////
        	s3_ping2.s3_ping2_log("PutObject");
        	String msg = "Timestamp: " + new Date() + " Msg: Hello World!";
        	byte[] data = msg.getBytes(); 
        	S3Object val = new S3Object(data, null);
        	HttpURLConnection httpConn = awsConn.put(bucketName, key, val, null).connection;
        	String rspmsg = httpConn.getResponseMessage();
        	int code = httpConn.getResponseCode();
        	String response = "" + code + " " + rspmsg;
        	s3_ping2.s3_ping2_log("PutObject: " + response);
        	
        		
		} catch (Exception e) {
			e.printStackTrace();
		}
        
    }
    
    @Test(dependsOnMethods = { "testListBucket" })
    public void testGetBucketLocation() {
    	
        try {
        	Assert.assertNotNull(awsConn);
        	///////////////////////////////////////////////
        	// getBucketLocation
        	///////////////////////////////////////////////
        	s3_ping2.s3_ping2_log("getBucketLocation --> listAllMyBuckets");
        	ListAllMyBucketsResponse bucket_list = awsConn.listAllMyBuckets(null);
        	String listAllMyBucketsResult;
        	List bucketList = null;
			if(!bucket_list.entries.isEmpty()){
				bucketList = bucket_list.entries;
				LocationResponse locationResponse = null;
				String sLocRsp = null;
				for (int i = 0; i < bucketList.size(); i++) {
		        	s3_ping2.s3_ping2_log("getBucketLocation --> listAllMyBuckets --> getBucketLocation");
					Bucket bucket = (Bucket)bucketList.get(i);
					s3_ping2.s3_ping2_log("GetBucketLocation[" + i + "] Name: " + bucket.name);
					s3_ping2.s3_ping2_log("GetBucketLocation[" + i + "] Creation Date: " + bucket.creationDate);
					locationResponse = awsConn.getBucketLocation(bucket.name);
					int contentLength = locationResponse.connection.getContentLength();
					sLocRsp = locationResponse.getLocation();
					s3_ping2.s3_ping2_log("GetBucketLocation: " + sLocRsp + " Bucket Name: " + bucket.name);
					
				}
        	}else{
        	}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
    }        	


    
    @Test(dependsOnMethods = { "testListBucket" })
    public void testGetBucketLogging() {
        try {
        	Assert.assertNotNull(awsConn);
        	///////////////////////////////////////////////
        	// GetBucketLogging
        	///////////////////////////////////////////////
        	s3_ping2.s3_ping2_log("GetBucketLogging");
        	GetResponse rsp = awsConn.getBucketLogging(bucketName, null);
        	String response;
        	if(rsp.object == null)
        		response = "bucketName " + bucketName + " GetBucketLogging not found";
        	else{
                byte[] buf=rsp.object.data;
                response = new String(buf);
        	}
            s3_ping2.s3_ping2_log("GetBucketLogging: " + response);
		} catch (Exception e) {
			e.printStackTrace();
		}
        
    }        	

    @Test(dependsOnMethods = { "testListBucket" })
    public void testGetBucketRequestPayment() {
        try {
        	Assert.assertNotNull(awsConn);
        	///////////////////////////////////////////////
        	// GetBucketRequestPayment
        	///////////////////////////////////////////////
        	s3_ping2.s3_ping2_log("GetBucketRequestPayment");
        	GetResponse rsp = awsConn.getBucketRequestPayment(bucketName, null);
        	String response;
        	if(rsp.object == null)
        		response = "bucketName " + bucketName + " BucketRequestPayment not found";
        	else{
                byte[] buf=rsp.object.data;
                response = new String(buf);
        	}
            s3_ping2.s3_ping2_log("GetBucketRequestPayment: " + response);
		} catch (Exception e) {
			e.printStackTrace();
		}
        
    }        	


    
    @Test(dependsOnMethods = { "testListBucket" })
    public void testGetAcl() {
        try {
        	Assert.assertNotNull(awsConn);
        	///////////////////////////////////////////////
        	// GetAcl
        	///////////////////////////////////////////////
        	s3_ping2.s3_ping2_log("GetAcl");
        	GetResponse rsp = awsConn.getACL(bucketName, key, null);
        	String response;
        	if(rsp.object == null)
        		response = "bucketName " + bucketName + "key " + key + " ACL not found";
        	else{
                byte[] buf=rsp.object.data;
                response = new String(buf);
        	}
            s3_ping2.s3_ping2_log("GetAcl: " + response);
		} catch (Exception e) {
			e.printStackTrace();
		}
        
    }        	

    @Test(dependsOnMethods = { "testPutObject" })
    public void testGetObject() {
    	
        try {
        	Assert.assertNotNull(awsConn);
        	///////////////////////////////////////////////
        	// GetObject
        	///////////////////////////////////////////////
        	s3_ping2.s3_ping2_log("GetObject");
        	GetResponse rsp = awsConn.get(bucketName, key, null);
        	String response;
        	if(rsp.object == null)
        		response = key + " in " + bucketName +" not found";
        	else{
                byte[] buf=rsp.object.data;
                response = new String(buf);
        	}
            s3_ping2.s3_ping2_log("GetObject: " + response);
		} catch (Exception e) {
			e.printStackTrace();
		}
        
    }
    


    @Test(dependsOnMethods = { "testGetObject" })
    public void testDeleteObject() {
    	
        try {
        	Assert.assertNotNull(awsConn);
        	///////////////////////////////////////////////
        	// DeleteObject
        	///////////////////////////////////////////////
        	s3_ping2.s3_ping2_log("DeleteObject");
        	HttpURLConnection httpConn = awsConn.delete(bucketName, key, null).connection;
        	String rspmsg = httpConn.getResponseMessage();
        	int code = httpConn.getResponseCode();
        	String response = "" + code + " " + rspmsg;
        	s3_ping2.s3_ping2_log("DeleteObject: " + response);
        	
        	s3_ping2.s3_ping2_log("DeleteObject (prefix)");
        	httpConn = awsConn.delete(bucketName, folder, null).connection;
        	rspmsg = httpConn.getResponseMessage();
        	code = httpConn.getResponseCode();
        	response = "" + code + " " + rspmsg;
        	s3_ping2.s3_ping2_log("DeleteObject: " + response);

        	
  		} catch (Exception e) {
			e.printStackTrace();
		}
        
    }

    ////////////////////////////////////////////////////////////
    // BUCKET
    ////////////////////////////////////////////////////////////


    @Test
    public void testCreateBucket() {
    	
    	if(bCreateBucket){
	    		
	        try {
	        	Assert.assertNotNull(awsConn);
	        	///////////////////////////////////////////////
	        	// CreateBucket
	        	///////////////////////////////////////////////
	        	s3_ping2.s3_ping2_log("CreateBucket");
	        	bucketName = "eu-central-1-" + System.currentTimeMillis();
	        	Response createBucketResponse = awsConn.createBucket(bucketName, null, null);
	        	HttpURLConnection httpConn = createBucketResponse.connection;
	        	int http_code = httpConn.getResponseCode();
	        	s3_ping2.s3_ping2_log("CreateBucket Response: " + httpConn.getResponseCode() + " " + httpConn.getResponseMessage());
	        	Assert.assertTrue(http_code == 200);
	        	bBucketCreated = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
    	}
        
    }        	

    @Test(dependsOnMethods = { "testDeleteObject" })
    public void testDeleteBucket() {
    	
    	if(bCreateBucket){
    		try {
    			Assert.assertNotNull(awsConn);
    			///////////////////////////////////////////////
    			// deleteBucket
    			///////////////////////////////////////////////
	        	s3_ping2.s3_ping2_log("deleteBucket");
    			HttpURLConnection httpConn = awsConn.deleteBucket(bucketName, null).connection;
    			String rspmsg = httpConn.getResponseMessage();
    			int code = httpConn.getResponseCode();
    			String response = "" + code + " " + rspmsg;
    			s3_ping2.s3_ping2_log("DeleteBucket: " + response);
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    	}
        
    }
    
    @Test(dependsOnMethods = { "testCreateBucket" })
    public void testCheckBucketExists() {
    	
        try {
        	Assert.assertNotNull(awsConn);
        	///////////////////////////////////////////////
        	// CheckBucketExists
        	///////////////////////////////////////////////
        	s3_ping2.s3_ping2_log("CheckBucketExists");
        	boolean bDoesExist = false;
        	boolean CheckBucketExists_rsp = awsConn.checkBucketExists(bucketName);
        	String CheckBucketExistsResult;
			if(CheckBucketExists_rsp){
				CheckBucketExistsResult = bucketName + " exists";
				bDoesExist = true;
        	}else{
        		CheckBucketExistsResult = bucketName + " does not exists";
        		bDoesExist = false;
        	}
        	s3_ping2.s3_ping2_log("CheckBucketExists: " + CheckBucketExistsResult);
        	Assert.assertTrue(bDoesExist);
		} catch (Exception e) {
			e.printStackTrace();
		}
        
    }
    
    @Test(dependsOnMethods = { "testPutObject" })
    public void testListBucket() {
    	
        try {
        	Assert.assertNotNull(awsConn);
        	///////////////////////////////////////////////
        	// listBucket
        	///////////////////////////////////////////////
        	s3_ping2.s3_ping2_log("listBucket");
        	ListBucketResponse bucket_rsp = awsConn.listBucket(bucketName, prefix, null, null, null);
        	String listBucketResult;
			if(!bucket_rsp.entries.isEmpty()){
				listBucketResult = bucket_rsp.entries.toString();
        	}else{
        		listBucketResult = "0 entries";
        	}
        	s3_ping2.s3_ping2_log("listBucket: " + listBucketResult);
		} catch (Exception e) {
			e.printStackTrace();
		}
        
    }

    @Test(dependsOnMethods = { "testCheckBucketExists" })
    public void testListAllMyBuckets() {
    	
        try {
        	Assert.assertNotNull(awsConn);
        	///////////////////////////////////////////////
        	// listAllMyBuckets
        	///////////////////////////////////////////////
        	s3_ping2.s3_ping2_log("listAllMyBuckets");
        	ListAllMyBucketsResponse bucket_list = awsConn.listAllMyBuckets(null);
        	String listAllMyBucketsResult;
			if(!bucket_list.entries.isEmpty()){
        		listAllMyBucketsResult = bucket_list.entries.toString();
        	}else{
        		listAllMyBucketsResult = "not found";
        	}
        	s3_ping2.s3_ping2_log("listAllMyBuckets: " + listAllMyBucketsResult);
		} catch (Exception e) {
			e.printStackTrace();
		}
        
    }

    

    ////////////////////////////////////////////////////////////
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidatePropertiesWithPreSignedPutSet() {
        s3_ping2.pre_signed_put_url = "http://s3.amazonaws.com/test-bucket/node1";
        s3_ping2.pre_signed_delete_url = null;
        s3_ping2.validateProperties();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidatePropertiesWithPreSignedDeleteSet() {
        s3_ping2.pre_signed_delete_url = "http://s3.amazonaws.com/test-bucket/node1";
        s3_ping2.pre_signed_put_url = null;
        s3_ping2.validateProperties();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidatePropertiesWithBothPreSignedSetButNoBucket() {
        s3_ping2.pre_signed_put_url = "http://s3.amazonaws.com/";
        s3_ping2.pre_signed_delete_url = "http://s3.amazonaws.com/";
        s3_ping2.validateProperties();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidatePropertiesWithBothPreSignedSetButNoFile() {
        s3_ping2.pre_signed_put_url = "http://test-bucket.s3.amazonaws.com/";
        s3_ping2.pre_signed_delete_url = "http://test-bucket.s3.amazonaws.com/";
        s3_ping2.validateProperties();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidatePropertiesWithBothPreSignedSetButTooManySubdirectories() {
        s3_ping2.pre_signed_put_url = "http://test-bucket.s3.amazonaws.com/subdir/DemoCluster/node1";
        s3_ping2.pre_signed_delete_url = "http://test-bucket.s3.amazonaws.com/subdir/DemoCluster/node1";
        s3_ping2.validateProperties();
    }
    
    @Test
    public void testValidatePropertiesWithBothPreSignedSetToValid() {
        s3_ping2.pre_signed_put_url = "http://test-bucket.s3.amazonaws.com/node1";
        s3_ping2.pre_signed_delete_url = "http://test-bucket.s3.amazonaws.com/node1";
        s3_ping2.validateProperties();
    }
    
    @Test
    public void testValidatePropertiesWithBothPreSignedSetToValidSubdirectory() {
        s3_ping2.pre_signed_put_url = "http://test-bucket.s3.amazonaws.com/DemoCluster/node1";
        s3_ping2.pre_signed_delete_url = "http://test-bucket.s3.amazonaws.com/DemoCluster/node1";
        s3_ping2.validateProperties();
    }
    
    @Test
    public void testUsingPreSignedUrlWhenNotSet() {
        Assert.assertFalse(s3_ping2.usingPreSignedUrls());
    }
    
    @Test
    public void testUsingPreSignedUrlWhenSet() {
        s3_ping2.pre_signed_put_url = "http://test-bucket.s3.amazonaws.com/node1";
        s3_ping2.pre_signed_delete_url = null;
        Assert.assertTrue(s3_ping2.usingPreSignedUrls());
    }
    
    @Test
    public void testGenerateQueryStringAuthenticationWithBasicGet() {
        String expectedUrl = "http://test-bucket.s3.amazonaws.com/node1?AWSAccessKeyId=abcd&Expires=1234567890&Signature=Khyk4bU1A3vaed9woyp%2B5qepazQ%3D";
        String encodedUrl =
            Utils.generateQueryStringAuthentication("abcd", "efgh", "get",
                                                    "test-bucket", "node1",
                                                    new HashMap(), new HashMap(),
                                                    1234567890);
        Assert.assertEquals(encodedUrl, expectedUrl);
    }
    
    @Test
    public void testGenerateQueryStringAuthenticationWithBasicPost() {
        String expectedUrl = "http://test-bucket.s3.amazonaws.com/node1?AWSAccessKeyId=abcd&Expires=1234567890&Signature=%2BsCW1Fc20UUvIqPjeGXkyN960sk%3D";
        String encodedUrl =
            Utils.generateQueryStringAuthentication("abcd", "efgh", "POST",
                                                    "test-bucket", "node1",
                                                    new HashMap(), new HashMap(),
                                                    1234567890);
        Assert.assertEquals(encodedUrl, expectedUrl);
    }
    
    @Test
    public void testGenerateQueryStringAuthenticationWithBasicPutAndHeaders() {
        Map headers = new HashMap();
        headers.put("x-amz-acl", Arrays.asList("public-read"));
        String expectedUrl = "http://test-bucket.s3.amazonaws.com/subdir/node1?AWSAccessKeyId=abcd&Expires=1234567890&Signature=GWu2Mm5MysW83YDgS2R0Jakthes%3D";
        String encodedUrl =
            Utils.generateQueryStringAuthentication("abcd", "efgh", "put",
                                                    "test-bucket", "subdir/node1",
                                                    new HashMap(), headers,
                                                    1234567890);
        Assert.assertEquals(encodedUrl, expectedUrl);
    }
    
    @Test
    public void testGeneratePreSignedUrlForPut() {
        String expectedUrl = "http://test-bucket.s3.amazonaws.com/subdir/node1?AWSAccessKeyId=abcd&Expires=1234567890&Signature=GWu2Mm5MysW83YDgS2R0Jakthes%3D";
        String preSignedUrl = S3_PING2.generatePreSignedUrl("abcd", "efgh", "put",
                                                           "test-bucket", "subdir/node1",
                                                           1234567890);
        Assert.assertEquals(preSignedUrl, expectedUrl);
    }
    
    @Test
    public void testGeneratePreSignedUrlForDelete() {
        String expectedUrl = "http://test-bucket.s3.amazonaws.com/subdir/node1?AWSAccessKeyId=abcd&Expires=1234567890&Signature=qbEMukqq0KIpZVjXaDi0VxepSVo%3D";
        String preSignedUrl = S3_PING2.generatePreSignedUrl("abcd", "efgh", "delete",
                                                           "test-bucket", "subdir/node1",
                                                           1234567890);
        Assert.assertEquals(preSignedUrl, expectedUrl);
    }

    @Test
    public void testPreSignedUrlParserNoPrefix() {
        PreSignedUrlParser parser = new PreSignedUrlParser("http://test-bucket.s3.amazonaws.com/node1");
        Assert.assertEquals(parser.getBucket(), "test-bucket");
        Assert.assertEquals(parser.getPrefix(), "");
    }

    @Test
    public void testPreSignedUrlParserWithPrefix() {
        PreSignedUrlParser parser = new PreSignedUrlParser("http://test-bucket.s3.amazonaws.com/subdir/node1");
        Assert.assertEquals(parser.getBucket(), "test-bucket");
        Assert.assertEquals(parser.getPrefix(), "subdir");
    }

    @Test
    public void testPreSignedUrlParserWithComplexBucketName() {
        PreSignedUrlParser parser = new PreSignedUrlParser("http://test-bucket.s3.foo-bar.s3.amazonaws.com/node1");
        Assert.assertEquals(parser.getBucket(), "test-bucket.s3.foo-bar");
        Assert.assertEquals(parser.getPrefix(), "");
    }
}

package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.protocols.S3_PING.Utils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Test(groups={Global.STACK_INDEPENDENT})
public class S3_PINGTest {
    private S3_PING ping;

    @BeforeMethod
    void setUp() {
        ping = new S3_PING();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidatePropertiesWithPreSignedPutSet() {
        ping.pre_signed_put_url = "http://s3.amazonaws.com/test-bucket/node1";
        ping.validateProperties();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidatePropertiesWithPreSignedDeleteSet() {
        ping.pre_signed_delete_url = "http://s3.amazonaws.com/test-bucket/node1";
        ping.validateProperties();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidatePropertiesWithBothPreSignedSetButNoBucket() {
        ping.pre_signed_put_url = "http://s3.amazonaws.com/";
        ping.pre_signed_delete_url = "http://s3.amazonaws.com/";
        ping.validateProperties();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidatePropertiesWithBothPreSignedSetButNoFile() {
        ping.pre_signed_put_url = "http://s3.amazonaws.com/test-bucket";
        ping.pre_signed_delete_url = "http://s3.amazonaws.com/test-bucket";
        ping.validateProperties();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidatePropertiesWithBothPreSignedSetButTooManySubdirectories() {
        ping.pre_signed_put_url = "http://s3.amazonaws.com/test-bucket/subdir/DemoCluster/node1";
        ping.pre_signed_delete_url = "http://s3.amazonaws.com/test-bucket/subdir/DemoCluster/node1";
        ping.validateProperties();
    }
    
    @Test
    public void testValidatePropertiesWithBothPreSignedSetToValid() {
        ping.pre_signed_put_url = "http://s3.amazonaws.com/test-bucket/node1";
        ping.pre_signed_delete_url = "http://s3.amazonaws.com/test-bucket/node1";
        ping.validateProperties();
    }
    
    @Test
    public void testValidatePropertiesWithBothPreSignedSetToValidSubdirectory() {
        ping.pre_signed_put_url = "http://s3.amazonaws.com/test-bucket/DemoCluster/node1";
        ping.pre_signed_delete_url = "http://s3.amazonaws.com/test-bucket/DemoCluster/node1";
        ping.validateProperties();
    }
    
    @Test
    public void testUsingPreSignedUrlWhenNotSet() {
        Assert.assertFalse(ping.usingPreSignedUrls());
    }
    
    @Test
    public void testUsingPreSignedUrlWhenSet() {
        ping.pre_signed_put_url = "http://s3.amazonaws.com/test-bucket/node1";
        Assert.assertTrue(ping.usingPreSignedUrls());
    }
    
    @Test
    public void testGenerateQueryStringAuthenticationWithBasicGet() {
        String expectedUrl = "http://s3.amazonaws.com/test-bucket/node1?AWSAccessKeyId=abcd&Expires=1234567890&Signature=Khyk4bU1A3vaed9woyp%2B5qepazQ%3D";
        String encodedUrl =
            Utils.generateQueryStringAuthentication("abcd", "efgh", "get",
                                                    "test-bucket", "node1",
                                                    new HashMap(), new HashMap(),
                                                    1234567890);
        Assert.assertEquals(encodedUrl, expectedUrl);
    }
    
    @Test
    public void testGenerateQueryStringAuthenticationWithBasicPost() {
        String expectedUrl = "http://s3.amazonaws.com/test-bucket/node1?AWSAccessKeyId=abcd&Expires=1234567890&Signature=%2BsCW1Fc20UUvIqPjeGXkyN960sk%3D";
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
        String expectedUrl = "http://s3.amazonaws.com/test-bucket/subdir/node1?AWSAccessKeyId=abcd&Expires=1234567890&Signature=GWu2Mm5MysW83YDgS2R0Jakthes%3D";
        String encodedUrl =
            Utils.generateQueryStringAuthentication("abcd", "efgh", "put",
                                                    "test-bucket", "subdir/node1",
                                                    new HashMap(), headers,
                                                    1234567890);
        Assert.assertEquals(encodedUrl, expectedUrl);
    }
    
    @Test
    public void testGeneratePreSignedUrlForPut() {
        String expectedUrl = "http://s3.amazonaws.com/test-bucket/subdir/node1?AWSAccessKeyId=abcd&Expires=1234567890&Signature=GWu2Mm5MysW83YDgS2R0Jakthes%3D";
        String preSignedUrl = S3_PING.generatePreSignedUrl("abcd", "efgh", "put",
                                                           "test-bucket", "subdir/node1",
                                                           1234567890);
        Assert.assertEquals(preSignedUrl, expectedUrl);
    }
    
    @Test
    public void testGeneratePreSignedUrlForDelete() {
        String expectedUrl = "http://s3.amazonaws.com/test-bucket/subdir/node1?AWSAccessKeyId=abcd&Expires=1234567890&Signature=qbEMukqq0KIpZVjXaDi0VxepSVo%3D";
        String preSignedUrl = S3_PING.generatePreSignedUrl("abcd", "efgh", "delete",
                                                           "test-bucket", "subdir/node1",
                                                           1234567890);
        Assert.assertEquals(preSignedUrl, expectedUrl);
    }
}
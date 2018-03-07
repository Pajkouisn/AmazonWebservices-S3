package pajkouisn.unittesting;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

import pajkouisn.Credentials.Keys.AWS;
import pajkouisn.utilities.amazonwebservices.S3;

public class TestPersist 
{
	static String testResourcePath = "src/test/resources/";
	static String fileName = "Sample.txt";
	@Test
	public void shouldRejectUnformedJson() throws Exception
	{
		S3 s3 = new S3(AWS.swfAccessId, AWS.swfSecretKey, AWS.region);
		s3.persistFileAsS3Object(AWS.bucketName, AWS.s3key, new File(String.format("%s%s", testResourcePath, fileName)));
	}
	
}

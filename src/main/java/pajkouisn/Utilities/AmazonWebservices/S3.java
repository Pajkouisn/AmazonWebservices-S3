package pajkouisn.Utilities.AmazonWebservices;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.Tag;

@SuppressWarnings("deprecation")
public class S3 
{	
	private AmazonS3 s3client;
	
	//	Default constructor
	public S3 () 
	{
		ClientConfiguration config = new ClientConfiguration();
		config.setMaxErrorRetry(10);
		s3client = AmazonS3ClientBuilder.standard().withClientConfiguration(config).withCredentials(new DefaultAWSCredentialsProviderChain()).build();
	}
	
	//	Parameterized constructor
	public S3 (String swfAccessId, String swfSecretKey) 
	{
		ClientConfiguration config = new ClientConfiguration();
		config.setMaxErrorRetry(10);
		s3client = AmazonS3ClientBuilder.standard().withRegion(System.getenv("Region")).withClientConfiguration(config).withCredentials((AWSCredentialsProvider) new StaticCredentialsProvider(new BasicAWSCredentials(swfAccessId, swfSecretKey))).build();
	}
	
	//	Parameterized constructor
	public S3 (String swfAccessId, String swfSecretKey, String region) 
	{
		ClientConfiguration config = new ClientConfiguration();
		config.setMaxErrorRetry(10);
		s3client = AmazonS3ClientBuilder.standard().withRegion(region).withClientConfiguration(config).withCredentials((AWSCredentialsProvider) new StaticCredentialsProvider(new BasicAWSCredentials(swfAccessId, swfSecretKey))).build();
	}
	
	/************************************************
	 * 	Setting Credentials for Amazon S3 Client.	*
	 ************************************************/
	
	//	Updates the client for Lambda services (Does not have instance credentials).
	public void setClientForLambda ()
	{
		ClientConfiguration config = new ClientConfiguration();
		config.setMaxErrorRetry(10);
		s3client = AmazonS3ClientBuilder.standard().withClientConfiguration(config).withCredentials(new DefaultAWSCredentialsProviderChain()).build();
	}
	
	//	Updates the client for EC2 using instance credentials.
	public void setClientForEC2 ()
	{
		ClientConfiguration config = new ClientConfiguration();
		config.setMaxErrorRetry(10);
		s3client = AmazonS3ClientBuilder.standard().withClientConfiguration(config).withCredentials(new InstanceProfileCredentialsProvider(false)).build();
	}
	
	//	Updates the client for EC2 by passing credentials.
	public void setClientWithCredentials (String swfAccessId, String swfSecretKey)
	{
		ClientConfiguration config = new ClientConfiguration();
		config.setMaxErrorRetry(10);
		s3client = AmazonS3ClientBuilder.standard().withRegion(System.getenv("Region")).withClientConfiguration(config).withCredentials((AWSCredentialsProvider) new StaticCredentialsProvider(new BasicAWSCredentials(swfAccessId, swfSecretKey))).build();
	}
	
	//	Updates the client for EC2 by passing credentials and region.
	public void setClientWithCredentials (String swfAccessId, String swfSecretKey, String region)
	{
		ClientConfiguration config = new ClientConfiguration();
		config.setMaxErrorRetry(10);
		s3client = AmazonS3ClientBuilder.standard().withRegion(region).withClientConfiguration(config).withCredentials((AWSCredentialsProvider) new StaticCredentialsProvider(new BasicAWSCredentials(swfAccessId, swfSecretKey))).build();
	}
	
	
	/************************************************
	 * 	Getting Credentials for Amazon S3 Client.	*
	 ************************************************/
	
	//	Returns a new Amazon S3 client for EC2 using instance credentials.
	public AmazonS3 getClientForEC2 ()
	{
		ClientConfiguration config = new ClientConfiguration();
		config.setMaxErrorRetry(10);
		AmazonS3 s3client = AmazonS3ClientBuilder.standard().withClientConfiguration(config).withCredentials(new InstanceProfileCredentialsProvider(false)).build();
		return s3client;
	}
	
	//	Returns a new Amazon S3 client for Lambda services (Does not have instance credentials).
	public AmazonS3 getClientForLambda ()
	{
		ClientConfiguration config = new ClientConfiguration();
		config.setMaxErrorRetry(10);
		AmazonS3 s3client = AmazonS3ClientBuilder.standard().withClientConfiguration(config).withCredentials(new DefaultAWSCredentialsProviderChain()).build();
		return s3client;
	}
	
	//	Updates the client for EC2 by passing credentials.
	public AmazonS3 getClientWithCredentials (String swfAccessId, String swfSecretKey)
	{
		ClientConfiguration config = new ClientConfiguration();
		config.setMaxErrorRetry(10);
		AmazonS3 s3client = AmazonS3ClientBuilder.standard().withRegion(System.getenv("Region")).withClientConfiguration(config).withCredentials((AWSCredentialsProvider) new StaticCredentialsProvider(new BasicAWSCredentials(swfAccessId, swfSecretKey))).build();
		return s3client;
	}
	
	//	Updates the client for EC2 by passing credentials and region.
	public AmazonS3 getClientWithCredentials (String swfAccessId, String swfSecretKey, String region)
	{
		ClientConfiguration config = new ClientConfiguration();
		config.setMaxErrorRetry(10);
		AmazonS3 s3client = AmazonS3ClientBuilder.standard().withRegion(region).withClientConfiguration(config).withCredentials((AWSCredentialsProvider) new StaticCredentialsProvider(new BasicAWSCredentials(swfAccessId, swfSecretKey))).build();
		return s3client;
	}
	
	/************************************************
	 * 	Getting object info							*
	 ************************************************/
	
	/*
	 * 	Get the object date
	 */
	public Date getObjectDate(String bucketName, String s3Key) 
	{
		// Split S3 Path to get the bucket name and the folder path.
		S3Object object = null;
				
		/*	Check if the object exists. 
		 * 	If the object retrieve the S3 object.
		 */
		if (s3client.doesObjectExist(bucketName, s3Key)) 
		{
			try 
			{
				object = s3client.getObject(new GetObjectRequest(bucketName, s3Key));
			} 
			
			catch (AmazonS3Exception e) 
			{
				System.out.println("Cannot get the datapoint we need.");
			}
		}
		
		
		return object.getObjectMetadata().getLastModified();
	}
	
	
	/*
	 * 	Get the object metadata
	 */
	public Map<String, String> getObjectUserMetadata(String bucketName, String s3Key) 
	{
		// Split S3 Path to get the bucket name and the folder path.
		S3Object object = null;
				
		/*	
		 * 	Check if the object exists. 
		 * 	If the object retrieve the S3 object.
		 */
		if (s3client.doesObjectExist(bucketName, s3Key)) 
		{
			try 
			{
				object = s3client.getObject(new GetObjectRequest(bucketName, s3Key));
			} 
			
			catch (AmazonS3Exception e) 
			{
				System.out.println("Cannot get the datapoint we need.");
			}
		}
		
		return object.getObjectMetadata().getUserMetadata();
	}
	
	/************************************************
	 * 	Read and write objects to and from S3.		*
	 ************************************************/
	
	/* 	
	 * 	This function is used to read the object from S3 as a String.
	 */
	public String readS3ObjectAsString (String bucketName, String s3Key) 
	{
        // Return the string.
        return s3client.getObjectAsString(bucketName, s3Key);
	}
	
	/*	
	 * 	This function is used to get S3 Object as a stream.
	 */
	public InputStream readS3ObjectAsStream (String bucketName, String s3Key)
	{		
		S3Object object = null;
		/*	Check if the object exists. 
		 * 	If the object retrieve the S3 object.
		 */
		if (s3client.doesObjectExist(bucketName, s3Key)) 
		{
			object = s3client.getObject(new GetObjectRequest(bucketName, s3Key));
		}
				
		InputStream objectData = object.getObjectContent();
		return objectData;
	}
	
	// 	Persist the String to S3.
	public void persistStringAsS3Object (String bucketName, String s3Key, String text)
    {
		s3client.putObject(bucketName, s3Key, text);
    }	
	
	// 	Persist the File to S3.
	public void persistFileAsS3Object (String bucketName, String s3Key, File file)
    {
		s3client.putObject(bucketName, s3Key, file);
    }
	
	// 	Persist the File with metadata to S3.
	public void persistFileAsS3ObjectWithMetadata (String bucketName, String s3Key, File file, String key, String value)
    {
		PutObjectRequest putRequest = new PutObjectRequest(bucketName, s3Key, file); 
		List<Tag> tags = new ArrayList<Tag>();
		tags.add(new Tag(key, value));
		putRequest.setTagging(new ObjectTagging(tags));
		s3client.putObject(putRequest);	
    }
	
	// 	Persist the File with metadata to S3.
	public void persistFileAsS3ObjectWithMetadata (String bucketName, String s3Key, File file, Map<String, String> tagMap)
    {
		PutObjectRequest putRequest = new PutObjectRequest(bucketName, s3Key, file); 
		List<Tag> tags = new ArrayList<Tag>();

		for(Entry<String, String> tag : tagMap.entrySet())
			tags.add(new Tag(tag.getKey(), tag.getValue()));
		putRequest.setTagging(new ObjectTagging(tags));
		s3client.putObject(putRequest);	
    }
	
	// 	Persist the File with metadata to S3.
	public void persistFileAsS3ObjectWithMetadata (String bucketName, String s3Key, File file, List<Tag> tags)
    {
		PutObjectRequest putRequest = new PutObjectRequest(bucketName, s3Key, file); 
		
		putRequest.setTagging(new ObjectTagging(tags));
		s3client.putObject(putRequest);	
    }
	
	// 	Persist the Stream to S3.
	public void persistStreamAsS3Object (String bucketName, String s3Key, InputStream content, int length) throws IOException
    {
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(length);
		s3client.putObject(new PutObjectRequest(bucketName, s3Key, content, metadata));
    }

	/************************************************
	 * 	Folder uploads to S3.						*
	 ************************************************/
	
	/* 	
	 * 	Upload an entire directory to S3, given the directory path and extensions.
	 */
	public void uploadFilesFromDirectory (String bucketName, String s3Key, String folderPath, String[] extensions)
    {
		// Split S3 Path to get the bucket name and the folder path.
		File local = new File(folderPath);
		List<File> files = (List<File>) FileUtils.listFiles(local, extensions, true);

		for (File file : files) 
		{
			s3client.putObject(bucketName, s3Key + "/" + file.getName(), file);
		} 
    }
	
	/* 	
	 * 	Upload an entire directory to S3, given the directory path and extensions.
	 */
	public void uploadFilesFromDirectory (String bucketName, String s3Key, List<File> files)
    {
		for (File file : files) 
		{
			s3client.putObject(bucketName, s3Key + "/" + file.getName(), file);
		} 
    }
	
	
	/* 	
	 * 	Upload an entire directory to S3, given the directory path and extensions.
	 * 	I use a windows PC hence changing \ to /.
	 */
	public void uploadDirectoryMaintainingStructure (String bucketName, String s3Key, String folderPath, String[] extensions)
    {
		// Split S3 Path to get the bucket name and the folder path.
		File local = new File(folderPath);
		List<File> files = (List<File>) FileUtils.listFiles(local, extensions, true);

		for (File file : files) 
		{
			s3client.putObject(bucketName, s3Key + "/" + file.getParent().replaceAll("\\\\", "/").replace(folderPath, ""), file);
		} 
    }
	
	
	/************************************************
	 * 	Copy objects to and from a S3 bucket/key.	*
	 ************************************************/
	
	/* 	
	 * 	This function is used to copy an object from a source S3 path, to target S3 path.
	 */
	public CopyObjectResult copyObject (String sourceBucketName, String sourceS3Key, String targetBucketName, String targetS3Key) 
	{
        // Return the string.
        return s3client.copyObject(sourceBucketName, sourceS3Key, targetBucketName, targetS3Key);
	}

	
	
	/************************************************
	 * 	Delete operations							*
	 ************************************************/		
	
	/*
	 * 	This function is used to delete an object from a S3 path
	 */
	public boolean deleteS3Object (String bucketName, String S3key) 
	{
		/*
		 * 	Check if the object exists. If the object retrieve the S3 object.
		 */
		if (s3client.doesObjectExist(bucketName, S3key)) 
		{
			try 
			{
				s3client.deleteObject(bucketName, S3key);
				return true;
			}

			catch (AmazonS3Exception e) 
			{
				return false;
			}
		}

		return true;
	}
	
	/*
	 * 	This function is used to delete all objects from a S3 folder.
	 */
	public int deleteAllS3Objects (String bucketName, String s3Key)
	{
		//	List all objects at the bucket and S3 Key.
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(s3Key);
        
		System.out.println("Deleting files from S3");
		
		/*	
		 * 	List all objects in s3 and get their summaries.
		 */
		ObjectListing objectListing;
		
		int i = 0;
		
		do
		{	
			objectListing = s3client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) 
			{
				if (deleteS3Object(objectSummary.getBucketName(), objectSummary.getKey()))	i++;
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		}while(objectListing.isTruncated());
		
		System.out.println("Objects deleted = " + i);
		
		return i;
	}
	
	/************************************************
	 * 	Listing operations							*
	 ************************************************/
	
	/*	
	 * 	A function  that lists all the buckets.
	 */
	public List<Bucket> listBuckets()
	{
		return s3client.listBuckets();
	}
	
	/*	
	 * 	A function  that lists all the buckets.
	 */
	public List<String> listBucketsAsString()
	{
		List<String> buckets = new ArrayList<String>();
		for(Bucket bucket : s3client.listBuckets())
		{
			buckets.add(bucket.getName());
		}
		return buckets;
	}
	
	
	/*	
	 * 	This function returns a list of all objects on S3.
	 */
	public List<String> listS3Objects (String bucketName, String s3Key)
	{
		List<String> objects = new ArrayList<String>();
		
		//	List all objects at the bucket and S3 Key.
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(s3Key);
        
		/*	
		 * 	List all objects in s3 and get their summaries.
		 */
		ObjectListing objectListing;
		
		int i = 0;

		do
		{	
			objectListing = s3client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) 
			{
				i++;
				objects.add("S3://"+objectSummary.getBucketName()+"/"+objectSummary.getKey());
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		}while(objectListing.isTruncated());
		
		System.out.println("Objects found = " + i);
		
		return objects;
	}
	
	
	/*	
	 * 	This function returns a list of all objects on S3.
	 */
	public List<String> listS3Objects (String bucketName, String s3Key, String delimiter)
	{
		List<String> objects = new ArrayList<String>();
		
		//	List all objects at the bucket and S3 Key.
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(s3Key).withDelimiter(delimiter);;
        
		/*	
		 * 	List all objects in s3 and get their summaries.
		 */
		ObjectListing objectListing;
		
		int i = 0;

		do
		{	
			objectListing = s3client.listObjects(listObjectsRequest);
			for (String objectSummary : objectListing.getCommonPrefixes()) 
			{
				i++;
				objects.add("S3://"+bucketName+"/"+objectSummary);
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		}while(objectListing.isTruncated());
		
		System.out.println("Objects Found = " + i);
		
		return objects;
	}
	
	
	/*	
	 * 	This function returns a list of all objects on S3.
	 */
	public List<String> listS3Objects (String bucketName, String s3Key, Date date)
	{
		List<String> objects = new ArrayList<String>();
		
		//	List all objects at the bucket and S3 Key.
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(s3Key);
        
		/*	
		 * 	List all objects in s3 and get their summaries.
		 */
		ObjectListing objectListing;
		
		int i = 0;

		do
		{	
			objectListing = s3client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) 
			{
				if(objectSummary.getLastModified().compareTo(date) >= 0)
				{
					i++;
					objects.add("S3://"+objectSummary.getBucketName()+"/"+objectSummary.getKey());
				}
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		}while(objectListing.isTruncated());
		
		System.out.println("Objects found = " + i);
		
		return objects;
	}
	
	
	/*	
	 * 	This function returns a list of all objects on S3.
	 */
	public List<String> listS3Objects (String bucketName, String s3Key, String delimiter, Date date)
	{
		List<String> objects = new ArrayList<String>();
		
		//	List all objects at the bucket and S3 Key.
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(s3Key).withDelimiter(delimiter);;
        
		/*	
		 * 	List all objects in s3 and get their summaries.
		 */
		ObjectListing objectListing;
		
		int i = 0;

		do
		{	
			objectListing = s3client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) 
			{
				if(objectSummary.getLastModified().compareTo(date) >= 0)
				{
					i++;
					objects.add("S3://"+objectSummary.getBucketName()+"/"+objectSummary.getKey());
				}
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		}while(objectListing.isTruncated());
		
		System.out.println("Objects Found = " + i);
		
		return objects;
	}
	

	
	/*	
	 * 	This function returns a list of all objects on S3.
	 */
	public List<String> listS3Objects (String bucketName, String s3Key, Date startDate, Date endDate)
	{
		List<String> objects = new ArrayList<String>();
		
		//	List all objects at the bucket and S3 Key.
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(s3Key);
        
		/*	
		 * 	List all objects in s3 and get their summaries.
		 */
		ObjectListing objectListing;
		
		int i = 0;

		do
		{	
			objectListing = s3client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) 
			{
				if(objectSummary.getLastModified().compareTo(startDate) >= 0 && objectSummary.getLastModified().compareTo(endDate) <= 0)
				{
					i++;
					objects.add("S3://"+objectSummary.getBucketName()+"/"+objectSummary.getKey());
				}
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		}while(objectListing.isTruncated());
		
		System.out.println("Objects found = " + i);
		
		return objects;
	}
	
	
	/*	
	 * 	This function returns a list of all objects on S3.
	 */
	public List<String> listS3Objects (String bucketName, String s3Key, String delimiter, Date startDate, Date endDate)
	{
		List<String> objects = new ArrayList<String>();
		
		//	List all objects at the bucket and S3 Key.
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(s3Key).withDelimiter(delimiter);;
        
		/*	
		 * 	List all objects in s3 and get their summaries.
		 */
		ObjectListing objectListing;
		
		int i = 0;

		do
		{	
			objectListing = s3client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) 
			{
				if(objectSummary.getLastModified().compareTo(startDate) >= 0 && objectSummary.getLastModified().compareTo(endDate) <= 0)
				{
					i++;
					objects.add("S3://"+objectSummary.getBucketName()+"/"+objectSummary.getKey());
				}
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		}while(objectListing.isTruncated());
		
		System.out.println("Objects Found = " + i);
		
		return objects;
	}
	
	
	/************************************************
	 * 	Counting operations							*
	 ************************************************/

	/*	
	 * 	This function returns a count of all objects in S3.
	 */
	public int countS3Objects (String bucketName, String s3Key)
	{
		//	List all objects at the bucket and S3 Key.
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(s3Key);

        /*	
		 * 	List all objects in s3 and get their summaries.
		 */
		ObjectListing objectListing;
		
		int i = 0;

		do
		{	
			objectListing = s3client.listObjects(listObjectsRequest);
			i += objectListing.getObjectSummaries().size();
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		}while(objectListing.isTruncated());
		
		System.out.println("Objects found = " + i);
		
		return i;
	}
	
	
	/*	
	 * 	This function returns a count of all objects on S3.
	 */
	public int countS3Objects (String bucketName, String s3Key, String delimiter)
	{
		//	List all objects at the bucket and S3 Key.
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(s3Key).withDelimiter(delimiter);;
        
        /*	
		 * 	List all objects in s3 and get their summaries.
		 */
		ObjectListing objectListing;
		
		int i = 0;

		do
		{	
			objectListing = s3client.listObjects(listObjectsRequest);
			i += objectListing.getObjectSummaries().size();
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		}while(objectListing.isTruncated());
		
		System.out.println("Objects Found = " + i);
		
		return i;
	}
	
	
	/************************************************
	 * 	Filtered operations							*
	 ************************************************/
	
	/*	
	 * 	This function returns a list of all objects on S3.
	 */
	public List<String> listFilteredS3Objects (String bucketName, String s3Key, String filter)
	{
		List<String> objects = new ArrayList<String>();
		
		//	List all objects at the bucket and S3 Key.
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(s3Key);
        
        /*	
		 * 	List all objects in s3 and get their summaries.
		 */
		ObjectListing objectListing;
		
		int i = 0;

		do
		{	
			objectListing = s3client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) 
			{
				if(objectSummary.getKey().contains(filter))
				{	
					i++;
					objects.add("S3://"+objectSummary.getBucketName()+"/"+objectSummary.getKey());
				}
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		}while(objectListing.isTruncated());
		
		System.out.println("Objects found = " + i);
		
		return objects;
	}
	
	
	/*	
	 * 	This function returns a list of all objects on S3.
	 */
	public List<String> listFilteredS3Objects (String bucketName, String s3Key, String delimiter, String filter)
	{
		List<String> objects = new ArrayList<String>();
		
		//	List all objects at the bucket and S3 Key.
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(s3Key).withDelimiter(delimiter);;
        
        /*	
		 * 	List all objects in s3 and get their summaries.
		 */
		ObjectListing objectListing;
		
		int i = 0;

		do
		{	
			objectListing = s3client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) 
			{
				if(objectSummary.getKey().contains(filter))
				{	
					i++;
					objects.add("S3://"+objectSummary.getBucketName()+"/"+objectSummary.getKey());
				}
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		}while(objectListing.isTruncated());
		
		System.out.println("Objects Found = " + i);
		
		return objects;
	}
	
	
	/*	A function  that lists all the buckets.
	 *
	 */
	public String findRequiredBucket (String filter)
	{
		for(Bucket bucket : s3client.listBuckets())
		{
			if(bucket.getName().contains(filter))	return bucket.getName();
		}
		
		return null;
	}
	
	/*	
	 * 	This function returns a list of all objects on S3.
	 */
	public String findRequiredS3Object (String bucketName, String s3Key, String filter)
	{
		//	List all objects at the bucket and S3 Key.
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(s3Key);
        
        /*	
		 * 	List all objects in s3 and get their summaries.
		 */
		ObjectListing objectListing;
		
		int i = 0;

		do
		{	
			objectListing = s3client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) 
			{
				i++;
				if(objectSummary.getKey().contains(filter))
				{	
					return "S3://"+objectSummary.getBucketName()+"/"+objectSummary.getKey();
				}
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		}while(objectListing.isTruncated());
		
		System.out.println("Objects found = " + i);
		
		return "";
	}
	
	
	/*	
	 * 	This function returns a list of all objects on S3.
	 */
	public String findRequiredS3Object (String bucketName, String s3Key, String delimiter, String filter)
	{
		//	List all objects at the bucket and S3 Key.
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(s3Key).withDelimiter(delimiter);;
        
        /*	
		 * 	List all objects in s3 and get their summaries.
		 */
		ObjectListing objectListing;
		
		int i = 0;

		do
		{	
			objectListing = s3client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) 
			{
				if(objectSummary.getKey().contains(filter))
				{	
					i++;
					return "S3://"+objectSummary.getBucketName()+"/"+objectSummary.getKey();
				}
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		}while(objectListing.isTruncated());
		
		System.out.println("Objects Found = " + i);
		
		return "";
	}
	
	/************************************************
	 * 	Check operations							*
	 ************************************************/
	
	/*	
	 * 	This function returns a boolean indicating whether the the object exists.
	 */
	public boolean doesObjectExist(String bucketName, String S3key )
	{
		return s3client.doesObjectExist(bucketName, S3key);
	}
	
	
	/*	
	 * 	This function returns a boolean indicating whether the the object exists.
	 */
	public boolean doesBucketExist (String bucketName)
	{
		return s3client.doesBucketExist(bucketName);
	}
		
	/************************************************
	 * 	get Unsigned URL for GET					*
	 ************************************************/
	
	/*	
	 * 	Get unsigned URL that is valid for x seconds (Only for get requests).
	 */
	public URL getUnsignedUrl(String bucketName, String S3key, int seconds)
	{
        java.util.Date expiration = new java.util.Date();
        long msec = System.currentTimeMillis();
        msec = msec + (seconds);
        expiration.setTime(msec);

        GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucketName, S3key);
        generatePresignedUrlRequest.setMethod(HttpMethod.GET);
        generatePresignedUrlRequest.setExpiration(expiration);

        URL url = s3client.generatePresignedUrl(generatePresignedUrlRequest);

        return url;
    }
	
	/************************************************
	 * 	Change object permissions.					*
	 ************************************************/
	/*	
	 * 	Make the object public.
	 */
	public void makePublic(String bucketName, String S3key)
	{
		s3client.setObjectAcl(bucketName, S3key, CannedAccessControlList.PublicRead);
	}

	/************************************************
	 * 	Check account info.							*
	 ************************************************/
	
	/*
	 * 	Get the account Owner Information.
	 */
	public Owner getS3AccountOwner() 
	{
		return s3client.getS3AccountOwner();
	}
}
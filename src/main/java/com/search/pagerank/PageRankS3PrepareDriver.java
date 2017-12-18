package com.search.pagerank;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class PageRankS3PrepareDriver extends Configured implements Tool{
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(); 
	    job.setJobName("PageRankReadFromS3");
	    job.setJarByClass(PageRankS3PrepareDriver.class);
	     
	    job.setMapperClass(PageRankS3PrepareMapper.class); //Set Mapper Class
	    job.setReducerClass(PageRankS3PrepareReducer.class); //Set Reducer Class 
	    
	    job.setMapOutputKeyClass(Text.class); //Set Mapper Output
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class); //Set Reducer Output
	    job.setOutputValueClass(Text.class);

        String bucketName = arg0[0];
        String inputFolder = arg0[1];
        String outputFolder = arg0[2];
        AmazonS3 s3client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());	       

        try {
            System.out.println("Listing objects");
            final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(inputFolder);//.withMaxKeys(2);
            ListObjectsV2Result result;
            do {               
               result = s3client.listObjectsV2(req);
               
               for (S3ObjectSummary objectSummary : 
                   result.getObjectSummaries()) {
                   if (!objectSummary.getKey().endsWith("/")) {
                       FileInputFormat.addInputPaths(job, "s3://" + bucketName + "/" + objectSummary.getKey());
                       System.out.println(" - " + objectSummary.getKey() + "  " +
                               "(size = " + objectSummary.getSize() + 
                               ")");
                   }
               }
               //System.out.println("Next Continuation Token : " + result.getNextContinuationToken());
               req.setContinuationToken(result.getNextContinuationToken());
            } while(result.isTruncated() == true ); 
            
         } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, " +
            		"which means your request made it " +
                    "to Amazon S3, but was rejected with an error response " +
                    "for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, " +
            		"which means the client encountered " +
                    "an internal error while trying to communicate" +
                    " with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }

	    FileOutputFormat.setOutputPath(job, new Path(outputFolder));
	    
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
 

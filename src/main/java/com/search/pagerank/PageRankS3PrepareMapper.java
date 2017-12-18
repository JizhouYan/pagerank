package com.search.pagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.S3Object;
 
//input :  S3 Line
//output: URL outlinks

public class PageRankS3PrepareMapper extends Mapper<LongWritable, Text, Text, Text>{
	static final String S3BUCKET_NAME = "cis555finalpagerank";
	static final String SEPARATOR = " ";
	
	private AmazonS3 s3Client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
		
    private String getStringFromInputStream(InputStream input) throws IOException {
    	// Read one text line at a time and display.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        StringBuilder sb = new StringBuilder();
        while (true) {
            String line = reader.readLine();
            if (line == null) break;
            sb.append(line);
            sb.append("\n");
        }
        return sb.toString();
    }
	
	private String getS3FileContent(String s3key) {
		S3Object s3object = s3Client.getObject(new GetObjectRequest(S3BUCKET_NAME, s3key));
		String content = null;
		try {
			content = getStringFromInputStream(s3object.getObjectContent());
			s3object.close();
		} catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which" +
            		" means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means"+
            		" the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        } catch (IOException e) {
        	e.printStackTrace();
		}
		return content;
	}
		
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("andyhemanthharijizhou");
        //should be 3 parts
        //url, [inputlinks], [outputlinks]
        //outlinks are divided using ","
        if (parts.length < 2) {
            System.out.println(line);
            return;
        }

        String url = parts[0];
        if (url.contains("\t")) {
            url = url.replaceAll("\t", "%09");
        }
        
        String[] outlinks = parts[2].split(",");
        for (String outlink : outlinks) {
            if (outlink.startsWith("https") || outlink.startsWith("http")) {
                outlink = outlink.replaceAll("\t", "%09");
                context.write(new Text(url), new Text(outlink));
            }
        }
	}
}

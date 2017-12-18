package com.search.pagerank;

import org.apache.hadoop.util.ToolRunner;

public class PageRankMain {

	static int iterations = 25;
	static String bucket;
	static String input;
	static String output;

	public static void main(String args[]) throws Exception {
		int flag = 0;
		//we get the correct parameters to start out program
		if (args.length == 0) {
    		bucket = "cis555finalpagerank";
	   	    input = "input_1";
		    output = "s3://cis555finalpagerank/output_1";
		} else if(args.length == 3) {
		    bucket = args[0];
		    input = args[1];
		    output = "s3://" + bucket + "/" + args[2];
		} else{
			System.err.println("The input parameters are not correct.");
			return;
		}
		
		System.err.println("The input folder: " + input);
		System.err.println("The output folder: " + output);
		//Start Reading S3 file 
		System.out.println("start reading s3 file");
		String preput = output + "/Step0_ReadS3File";
		String[] initParas = new String[] {bucket, input, preput};
		flag = ToolRunner.run(new PageRankS3PrepareDriver(), initParas);
		
		if (flag == 1) { //fail
			System.out.println("reading s3 file has some problems!!!");
			System.exit(flag);
		}
        		
		System.out.println("Finishing Reading");
		//System.exit(0);

		String iterIn = "";
		String iterOut = "";
		//Start Main PageRank Procedure
		for (int i = 0; i < iterations; i++) {
			iterIn = (i == 0? preput : iterOut);
			iterOut = output + "/Step1_PageRank" + String.valueOf(i);
			System.out.println("Current is Round :" + i);
			String[] paras = new String[] { iterIn, iterOut };
			flag = ToolRunner.run(new PageRankMainDriver(), paras);
			if (flag == 1) { //we need flag = 0
				System.err.println("PageRank failed at Iteration: " + i);
				System.exit(flag);
			}
		}

		//Run PageRank Finalize Driver
		String[] lastParas = new String[] { iterOut , output + "/output" };
		flag = ToolRunner.run(new PageRankFinalizeDriver(), lastParas);
		if (flag == 1) { System.exit(flag); }
		
		System.out.println("Finishing PageRank Correctly :)");
		System.exit(0);
	}
}

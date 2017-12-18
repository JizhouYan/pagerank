package com.search.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//input :  URL	  Score|1.0 outurl1 outurl2 ....
//output1: URL    PageRankScore(current round)
//get each single one, combine and process it in reducer
//output2: URL 	  Score|1.0 outurl1 outurl2 .... 

public class PageRankMainMapper extends Mapper<LongWritable,Text,Text,Text> {
	@Override 
	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		String[] urls = values.toString().split("\t"); //based on the tab sign I stored
		String curURL = "";
		if(urls.length != 2) { 
			return;
		} else { //no tab in our url
		//urls[0] curURL
		//urls[1] Score|1.0 outURL1 outURL2 outURL3
		//urls[1] 0.55555
		//in order to get all the ourURLs in our reducer, we need to do this extra write
		try {
			curURL = urls[0];
			if(urls[1].startsWith("Score|") == false) {
				System.out.println("Pay attention here, this url:" + curURL + "The part needs to repair is: " + urls[1]);
			}
			context.write(new Text(curURL), new Text(urls[1]));	
		
			String scoreAndLinks[]= urls[1].split("\002", 2);  
			double averageScore = 0.0;
			double curPageRankScore = Double.parseDouble(scoreAndLinks[0].split("\\|")[1]); //should eliminate "Score|"
			if(scoreAndLinks.length != 2) { 
				averageScore = curPageRankScore; 
				context.write(new Text(curURL), new Text(String.valueOf(averageScore))); //we emit current URL with avarageScore
			} else { 
				String outURLstring = scoreAndLinks[1];
				String outURLs[] = outURLstring.split("\002"); 
				int outURLNum = outURLs.length;
				averageScore = curPageRankScore / outURLNum;
				for(int i = 0; i < outURLNum; i++) {
					context.write(new Text(outURLs[i]), new Text(String.valueOf(averageScore)));
				}	
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	 }
 	}
}
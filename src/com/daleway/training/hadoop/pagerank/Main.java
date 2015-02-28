package com.daleway.training.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {
	

	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf1, args)
				.getRemainingArgs();
		
		if(otherArgs.length < 5){
			System.err.println("Usage: Main <type=simple|complete> <in> <temp> <out> <NUMBEROFITERATIONS>");
			System.exit(2);			
		}
			
		Job job1 = PageRankAdjList.createJob(conf1, otherArgs[1], otherArgs[2]);		
		if(!job1.waitForCompletion(true)){
			System.exit(1);	
		}
		
		int numberOfIterations = Integer.parseInt(args[4]);
		String input= null, output= null;
		for (int i = 0; i < numberOfIterations ; i++) {
			if(i==0){
				input = otherArgs[2];
			}else {
				input = output;
			}
			output = otherArgs[2] + "-"+ i;
						
			if(otherArgs[0].equalsIgnoreCase("simple")){
				job1 = PageRankSimple.createJob(new Configuration(), input, output);
			}else if(otherArgs[0].equalsIgnoreCase("complete")){
				job1 = PageRankCalcDanglingNodeMass.createJob(conf1, input, output + "-d");
				if(!job1.waitForCompletion(true)){
					System.exit(1);	
				}
				long val = job1.getCounters().findCounter(DanglingNodeMass.Counter).getValue();
				System.out.println("got counter = "+ val);
				double vald = (double)val/100000000.0;
				System.out.println("got counter = "+ vald);
				conf1.set("dangling.node.mass", ""+vald);				
				job1 = PageRankComplete.createJob(conf1, input, output);
			}else{
				System.err.println("Usage: Main <type=simple|complete> <in> <temp> <out> <NUMBEROFITERATIONS>");				
				System.exit(2);			
			}
			if(!job1.waitForCompletion(true)){
				System.exit(1);	
			}
		}
		input=null;
		output=null;					
		input = otherArgs[2] + "-" + (numberOfIterations -1);
		output = otherArgs[3] + "-"+ (numberOfIterations -1);
		Configuration conf2 = new Configuration();
		Job job2 = PageRankSecondarySort.createJob(conf2, input, output);
		if(!job2.waitForCompletion(true)){
			System.exit(1);
		}
	}
}
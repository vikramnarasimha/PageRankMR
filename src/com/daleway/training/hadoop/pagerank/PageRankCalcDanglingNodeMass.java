/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.daleway.training.hadoop.pagerank;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRankCalcDanglingNodeMass {
	
	public static class PageRankMapper extends
			Mapper<Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] t = value.toString().split("\t");
			String[] adjlist= t[1].split(",");
			double pr = Double.parseDouble(t[2])/adjlist.length;
			context.write(new Text(t[0]), new Text(t[1] + "\t" + t[2]));
			
			for (int i = 0; i < adjlist.length; i++) {
				context.write(new Text(adjlist[i]), new Text(""+pr));						
			}
		}
	}
	
	public static class PageRankReducer extends
			Reducer<Text, Text, Text, Text> {
		double s = 0.0;					
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			if(key.toString().equals("")){

				String t[] = null;
				Text graph = new Text();
				for (Text text : values) {
					t = text.toString().split("\t");
					if(t.length == 2){
						graph.set(t[0]);
					}else{				
						try{
							s += Double.parseDouble(text.toString());	
						}catch(NumberFormatException e){}					
					}				
				}
				context.write(key, new Text(""+s));
			}	
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {

			long val = (long)(100000000 * s);
			
			System.out.println("Setting counter = " + val + " USING dn = " + s);
			context.getCounter(DanglingNodeMass.Counter).setValue(val);						
		}
	}
	
	
	
	public static Job createJob(Configuration conf, String inputPath, String outputPath) throws IOException{
		Job job = new Job(conf, "pair wise count");
		job.setJarByClass(PageRankCalcDanglingNodeMass.class);
		job.setMapperClass(PageRankMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(PageRankReducer.class);		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		return job;
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: pairwise <in> <out>");
			System.exit(2);
		}
		System.exit(createJob(conf, otherArgs[0], otherArgs[1]).waitForCompletion(true) ? 0 : 1);
	}
}
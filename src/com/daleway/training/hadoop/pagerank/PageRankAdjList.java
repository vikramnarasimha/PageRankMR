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
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRankAdjList {
	
	public static class PageRankMapper extends
			Mapper<Object, Text, Text, Text> {

		public void map(Object nkey, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] t = value.toString().split("\t");
			if(t.length == 2){
				String from = t[0];
				String to = t[1];
				context.write(new Text(from), new Text(to));
			}
		}
	}
	
	public static class PageRankReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			String x = "";
			for(Text text: values){
				if(x.equals("")){
					x += text.toString();
				}else{
					x += "," + text.toString();					
				}
			}
			
			context.write(key, new Text(x + "\t" + new DecimalFormat("#.00000000").format(((double)1/75879.0))));
		}
	}
	
	public static Job createJob(Configuration conf, String inputPath, String outputPath) throws IOException{
		Job job = new Job(conf, "pair wise count");
		job.setJarByClass(PageRankAdjList.class);
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
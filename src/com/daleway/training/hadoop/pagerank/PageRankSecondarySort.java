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
/*
 * I initially thought I had to do the Secondary Sort within the same MapReduce. 
 * The only reason we need the reducer here is for stripping out the composite key. 
 * (Don't think this can be achieved in the mapper)
 */
package com.daleway.training.hadoop.pagerank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRankSecondarySort {

	public static class TokenizerMapper extends
			Mapper<Object, Text, LongWritable, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] s = value.toString().split("\t");
			double v = Double.parseDouble(s[2]);
			context.write(new LongWritable((long)(100000000 * v)),
					new Text(s[0] +"\t" +s[2]));
		}
	}

	public static class IntSumReducer extends Reducer<LongWritable, Text, Text, Text> {

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			System.out.println(key);
			for (Text text : values) {
				String t[] = text.toString().split("\t");
				context.write(new Text(t[0]), new Text(t[1]));
			}
		}
	}

	public static Job createJob(Configuration conf, String inputPath,
			String outputPath) throws IOException {
		Job job = new Job(conf, "pair wise count");
		job.setJarByClass(PageRankSecondarySort.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setSortComparatorClass(LongWritable.DecreasingComparator.class	);
		job.setMaxReduceAttempts(1);
		job.setNumReduceTasks(1);

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
		System.exit(createJob(conf, otherArgs[0], otherArgs[1])
				.waitForCompletion(true) ? 0 : 1);
	}
}
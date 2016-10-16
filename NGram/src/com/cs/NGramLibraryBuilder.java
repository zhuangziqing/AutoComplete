package com.cs;

import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		int noGram;
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			noGram = conf.getInt("noGram", 5);
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			line = line.trim().toLowerCase();
			line = line.replaceAll("[^a-z]", " ");
			
			String[] words = line.split("\\s+"); //split by ' ', '\t' ... etc
			if(words.length < 2) {
				return;
			}
			
			/*
			// two for loop to write n = 2, n= 3, n= ...  string to output file
			//For example: I love big data -> (I love \t 1) (love big \t 1) (big data \t 1)
			//(I love big \t 1) (love big data \t 1) (I love big data \t 1)
			 * 
			 */
			StringBuilder sb;
			for(int i = 0; i < words.length - 1; i++) {
				// logic about words.length
				sb = new StringBuilder();
				sb.append(words[i]);
				for(int j = 1; i + j < words.length && j < noGram; j++) {
					sb.append(" ");
					sb.append(words[i+j]);
					context.write(new Text(sb.toString().trim()), new IntWritable(1));
				}
			}
		}
	}
	
	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}

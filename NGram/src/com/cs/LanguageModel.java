package com.cs;

import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LanguageModel {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, Text> {
		//Some of the ngram frequency is very low, there is not chance that we may use it. So we set
		// a threashold to filter out those low frequency phrases.
		int threashold;
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			threashold = conf.getInt("threashold", 20);
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim().length() == 0)) {
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			// For example: input    I love \t 1
			// I love -> wordsPlusCount[0]   
			// 1 -> wordsPlusCount[1]
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);
			
			//If the count number is less than threashold, then skip it.
			if(count < threashold) {
				return;
			}
			
			// This step divide n words string to   n-1 words + last word
			// For example: "this is cool" -> this is + cool
			// "this is" will be key, and "cool" + count will be value
			//this is  --> cool = 20
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < words.length - 1; i++) {
				sb.append(words[i]).append(" ");
			}
			String outputKey = sb.toString().trim();
			String outputValue = words[words.length - 1];
			
			if(!((outputKey == null) || (outputKey.length() < 1))) {
				context.write(new Text(outputKey),  new Text(outputValue + "=" + count));
			}
		}
	}	
	public static class NGramReducer extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
		int n;
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// In reducer, we want to combine same key together.
			// For example: key "this is" value "boy = 60"	key "this is" value "girl = 50"
			// -> this is,  <girl = 50, boy = 60>
			TreeMap<Integer, List<String>> tm = new TreeMap<>(Collections.reverseOrder());
			for(Text val : values) {
				String curValue = val.toString().trim();
				// first part is string, second part is count;
				String word = curValue.split("=")[0].trim();
				int count = Integer.parseInt(curValue.split("=")[1].trim());
				if(tm.containsKey(count)) {
					tm.get(count).add(word);
				}
				else {
					List<String> list = new ArrayList<>();
					list.add(word);
					tm.put(count, list);
				}
			}
			// TreeMap will organize all the data to ->
			// <50, <girl, bird>>  <60, <boy..>>
			// get the key, 
			// We only select top n most frequently show phrase will write them to database.
			Iterator<Integer> iter = tm.keySet().iterator();
			for(int j = 0; iter.hasNext() && j < n; j++) {
				int keyCount = iter.next();
				List<String> words = tm.get(keyCount);
				
				for(String curWord: words) {
					context.write(new DBOutputWritable(key.toString(), curWord, keyCount), NullWritable.get());
					j++;
				}
			}
		}
		
	}
}

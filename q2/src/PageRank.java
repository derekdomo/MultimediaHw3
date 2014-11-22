//package org.myorg;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class PageRank {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			// Ignore comments in the dataset
			if(line.startsWith("#")) {
				return;
			}
			Text list = new Text();
			StringTokenizer tokenizer = new StringTokenizer(line);
			word.set(tokenizer.nextToken());
			list.set(tokenizer.nextToken());
			output.collect(word, list);
			output.collect(list, new Text("one"));
			output.collect(word, new Text("one"));
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String list="";
			while (values.hasNext()) {
				String val = values.next().toString();
				if (!val.equals("one")) {
					output.collect(new Text(val+"#"+key),new Text("1"));
					list+="|"+val;
				}
			}
			Text l = new Text();
			l.set(list);
			output.collect(key, new Text("1"));
			String keyPR = key.toString();
			if (!list.equals("")) 
				output.collect(new Text("."+keyPR),l);
		}
	}
	
	public static class MapIter extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String[] values=value.toString().split("\t");
			String line = values[1];
			if (isNumeric(line)&&!values[0].contains("#"))
				output.collect(new Text(values[0]), new Text(line));
			else {
				if (values[0].contains(".")) {
					String[] tt = line.split("\\|");
					String keyPR = values[0].substring(1);
					for (String it:tt) {
						Text tmp = new Text();
						if (it.equals(""))
							continue;
						tmp.set("~"+it);
						output.collect(new Text(keyPR), tmp);
					}
				}
				else if (values[0].contains("#")) {
					String[] tt = values[0].split("#");
					String keyPR = tt[0];
					output.collect(new Text(keyPR), new Text(values[1]));
				}
			}
		}
	}
	
	public static class ReduceIter extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			int sum = 0;
			String list="";
			List<String> out=new ArrayList<String>();
			String keyPR = key.toString();
			while (values.hasNext()) {
				String val = values.next().toString();
				if (isNumeric(val))
					sum+=Integer.parseInt(val);
				else {
					list+="|"+val.substring(1);
					out.add(val.substring(1));
				}
			}
			Text s = new Text();
			s.set(String.valueOf(sum));
			if (!list.equals(""))
				output.collect(new Text("."+keyPR),new Text(list));
			for (String it : out) {
				output.collect(new Text(it+"#"+keyPR), s);	
			}
			output.collect(key, s);
		}
	}
	
	public static boolean isNumeric(String num) {
		if (num != null && !"".equals(num.trim()))  
            return num.matches("^[0-9]*$");  
        else  
            return false; 
	}

	public static void main(String[] args) throws Exception {
		//Initiate
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("PageRank");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path("/users/xiangyus/outputInit"));

		JobClient.runJob(conf);
		String prePath = "/users/xiangyus/outputInit";
		String current = "/users/xiangyus/outputIter";
		//Iteration
		for (int i=0; i<5; i++) {
			JobConf confIter = new JobConf(PageRank.class);
			confIter.setJobName("Iteration"+String.valueOf(i));
			confIter.setOutputKeyClass(Text.class);
			confIter.setOutputValueClass(Text.class);
			confIter.setMapperClass(MapIter.class);
			confIter.setReducerClass(ReduceIter.class);
			confIter.setInputFormat(TextInputFormat.class);
			confIter.setOutputFormat(TextOutputFormat.class);
			FileInputFormat.setInputPaths(confIter, new Path(prePath));
			FileOutputFormat.setOutputPath(confIter, new Path(current+String.valueOf(i)));
			JobClient.runJob(confIter);
			prePath=current+String.valueOf(i);
		}
	}
}

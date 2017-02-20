package com.okmich.dezyre.combinefileinput;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SmallFileReadJob extends Configured implements Tool {

	private static class MyMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		if (args.length < 2)
			System.exit(-2);
		try {
			ToolRunner.run(new SmallFileReadJob(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance();
		job.setJarByClass(SmallFileReadJob.class);
		job.setMapperClass(MyMapper.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(CombineFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		String[] arg0s = new GenericOptionsParser(getConf(), arg0)
				.getRemainingArgs();
		FileInputFormat.setInputPaths(job, new Path(arg0s[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0s[1]));

		return job.waitForCompletion(true) ? 1 : 0;
	}

}

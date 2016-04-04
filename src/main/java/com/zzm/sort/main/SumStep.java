package com.zzm.sort.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zzm.sort.domain.InfoBean;

public class SumStep {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SumStep.class);
		job.setMapperClass(SumMapper.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InfoBean.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
	
	public static class SumMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
		
		private Text k = new Text();
		
		private InfoBean v = new InfoBean();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, InfoBean>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			String account = fields[0];
			double income = Double.parseDouble(fields[1]);
			double expenses = Double.parseDouble(fields[2]);
			k.set(account);
			v.set(account, income, expenses);
			context.write(k, v);
		}
		
	}
	
	public static class SumReducer extends Reducer<Text, InfoBean, Text, InfoBean> {
		
		private InfoBean v = new InfoBean();
		
		@Override
		protected void reduce(Text key, Iterable<InfoBean> value, Context context)
				throws IOException, InterruptedException {
			double in_sum = 0;
			double out_sum = 0;
			for (InfoBean bean : value) {
				in_sum += bean.getIncome();
				out_sum += bean.getExpenses();
			}
			v.set("", in_sum, out_sum);
			context.write(key, v);
		}
		
	}
}

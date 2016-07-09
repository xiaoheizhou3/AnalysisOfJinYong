package pageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.StringTokenizer;

public class Sort {
	public static class SortMapper extends Mapper<Object, Text, DoubleWritable, Text> {
		protected void map(Object key, Text value,Context context)throws IOException,InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()){
				String tmp1 = itr.nextToken();
				String tmp2 = itr.nextToken();
				context.write(new DoubleWritable(Double.parseDouble(tmp2)), new Text(tmp1));
			}
		}
	}	

	public static class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {
		protected void reduce(DoubleWritable key, Iterable<Text> values, Context context )throws IOException,InterruptedException {
			for(Text t : values)
				context.write(t, new Text("  " + key.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        @SuppressWarnings("deprecation")
		Job job=new Job(conf);
        job.setJarByClass(Sort.class);
        job.setReducerClass(SortReducer.class);
        job.setMapperClass(SortMapper.class);
        
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
	}
}

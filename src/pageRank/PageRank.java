package pageRank;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.StringTokenizer;

public class PageRank {
	
	public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {
		protected void map(Object key, Text value,Context context)throws IOException,InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()){
				String tmp = itr.nextToken();
				String[] namePrList = tmp.split(" | ");
				for(int i = 0; i < namePrList.length; i++){
					String[] namePr = namePrList[i].split(",");
					if(namePr.length > 1)
						context.write(new Text(namePr[0]), /*new Text("0.0")*/new Text(namePr[1]));
				}
			}
		}
	}	

	public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context )throws IOException,InterruptedException {
			double pr = 0.0;
			for(Text t : values){
				pr += Double.parseDouble(t.toString());
			}
			//context.write(key, new Text((pr * 0.85 + 0.15) + ""));
			context.write(new Text(pr * 0.85 + 0.15 + ""), key);
		}
	}
	
	public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        @SuppressWarnings("deprecation")
		Job job=new Job(conf);
        job.setJarByClass(PageRank.class);
        job.setReducerClass(PageRankReducer.class);
        job.setMapperClass(PageRankMapper.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
	}
}

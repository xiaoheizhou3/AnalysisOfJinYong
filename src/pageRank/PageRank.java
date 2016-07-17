package pageRank;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

public class PageRank {

	public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tuple = line.split("\t");//tuple[0]为人名,tuple[1]为上一轮迭代的rank值,tuple[2]为人名列表(B | weight)	
			if (tuple.length > 2) {
				String A = tuple[0];
				double pr = Double.parseDouble(tuple[1]);
				String[] array = tuple[2].split(" | ");
				for (int i = 0; i < array.length; i++){
					String[] tmp = array[i].split(",");
					if(tmp.length >= 2){
						String name = tmp[0];
						double linkPr = Double.parseDouble(tmp[1]);
						String prValue = A + "," + pr * linkPr;//计算出A给name投的票数
						context.write(new Text(name), new Text(prValue));
					}
				}
				context.write(new Text(A), new Text("#" + tuple[2]));
			}
		}
	}
	
	public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String links = "";
			double pagerank = 0.0;
			
			for(Text value : values){
				String tmp = value.toString();
				if(tmp.startsWith("#")) {
					links = "\t" + tmp.substring(tmp.indexOf("#") + 1);
					continue;
				}
				String[] tuple = tmp.split(",");
				if(tuple.length > 1){
					pagerank += Double.parseDouble(tuple[1]);
				}
			}
			pagerank = pagerank * 0.85 + 0.15;
			context.write(new Text(key), new Text(String.valueOf(pagerank) + links));
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
        job.waitForCompletion(true);
	}
}

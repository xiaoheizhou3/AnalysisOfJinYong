package pageRank;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
 * 计算一轮pagerank值，对输出格式进行调整，为pagerank算法的多次迭代做准备
 */
public class PrePageRank{
	
	public static class PrePageRankMapper extends Mapper<Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			/*
			 * value的格式为A B,weight | C,weight | D,weight |
			 * 以Tab符对value做分割，tuple[0]为A，tuple[1]为后面一串人名列表
			 */
			String line = value.toString();
			String[] tuple = line.split("\t");
			if(tuple.length >= 2){
				String A = tuple[0];
				String[] array = tuple[1].split(" | ");//以" | "对人名列表做分割，分割出一个个形如B,weight的单元，存储在array中
				
				for (int i = 0; i < array.length; i++) {
					String[] tmp = array[i].split(",");//将B,weight分割为B和weight
					if(tmp.length >= 2){
						String name = tmp[0];
						double linkPr = Double.parseDouble(tmp[1]);//初始投票值为weight
						String prValue = A + "," + linkPr;
						context.write(new Text(name), new Text(prValue));//(名字，A给这个人投的票数)
					}
				}
				context.write(new Text(A), new Text("#" + tuple[1]));//A的名字，#，和A关联的人及比重
			}
		}
	}

	public static class PrePageRankReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String links = "";
			double pagerank = 0.0;
			for (Text value : values) {
				String tmp = value.toString();
				if (tmp.startsWith("#")) {
					links = "\t" + tmp.substring(tmp.indexOf("#") + 1);//重新构造出与key这个人关联的人名列表
					continue;
				}
				String[] tuple = tmp.split(",");
				if (tuple.length > 1) {
					pagerank += Double.parseDouble(tuple[1]);//统计所有人给key这个人投的票数
				}
			}
			pagerank = pagerank * 0.85 + 0.15;//计算出rank值
			context.write(key, new Text(String.valueOf(pagerank) + links));
		}
	}
    
	public static void main(String[] args) throws Exception{        
        Configuration conf=new Configuration();
        @SuppressWarnings("deprecation")
		Job job=new Job(conf);
        job.setJarByClass(PageRank.class);
        job.setReducerClass(PrePageRankReducer.class);
        job.setMapperClass(PrePageRankMapper.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
	}
}
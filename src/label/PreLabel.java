package label;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PreLabel {
	
	public static class PreLabelMapper extends Mapper<Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tuple = line.split("\t");
			String keyName = tuple[0];
			StringBuffer newLink = new StringBuffer();
			/*原版标签传播，可用
			if(tuple.length >= 2){
				String[] array = tuple[1].split(" | ");
				for (int i = 0; i < array.length; i++){
					String[] tmp = array[i].split(",");
					if(tmp.length >= 2){
						newLink.append(tmp[0] + " | ");//B | C | D |
					}
				}
				//输出格式为A	 A B | C | D |
				context.write(new Text(keyName), new Text("#" + keyName.toString() + "#\t" + newLink.toString()));
			}
			*/
			//加强版标签传播，加入了rank值
			if(tuple.length >= 2){
	        	//读取人名rank值文件，该文件在main方法中已经加载到distributed Cache中
	        	@SuppressWarnings("deprecation")
				Path[] localFiles = context.getLocalCacheFiles();
	        	FileReader fr = new FileReader(localFiles[0].toString());
	            BufferedReader br = new BufferedReader(fr);
	            //按行读取人名列表，加入到map中
	            HashMap<String, Double> map = new HashMap<>();
	            String nameRank = new String();
	            while((nameRank = br.readLine()) != null){
	            	String[] name_rank = nameRank.split("\t");//分割出人名和rank值
	            	map.put(name_rank[0], Double.valueOf(name_rank[1]));
	            }
	            br.close();
	            
				String[] array = tuple[1].split(" | ");
				for (int i = 0; i < array.length; i++){
					String[] tmp = array[i].split(",");
					if(tmp.length >= 2){
						newLink.append(tmp[0] + "," + map.get(tmp[0]) + " | ");//B,rank | C,rank | D,rank |
					}
				}
				//输出格式为A	 A B,rank | C,rank | D,rank |
				String name = keyName.toString();
				context.write(new Text(keyName), new Text("#" + name + "," + map.get(name) + "#\t" + newLink.toString()));
			}
		}
	}

	public static class PreLabelReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for(Text value : values)	context.write(key, new Text(value));
		}
	}
    
	public static void main(String[] args) throws Exception{        
        Configuration conf=new Configuration();
        @SuppressWarnings("deprecation")
		Job job=new Job(conf);
        //三个参数分别为输入文件（wordcut的结果），输出路径, pagerank的结果
        job.addCacheFile(new Path(args[2]).toUri());
        
        job.setJarByClass(PreLabel.class);
        job.setReducerClass(PreLabelReducer.class);
        job.setMapperClass(PreLabelMapper.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
	}
}
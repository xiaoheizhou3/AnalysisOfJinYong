package wordCut;

import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
/*
 * 
	WordCut类一次性完成题目要求的任务一、任务二、任务三，一趟就能跑完，算是优化。
	导出jar包的方式：在src文件夹右键，选择export，选择java，选择Runable JAR file，选择WordCut类,命名为wordCut.jar
	运行方式：登录到集群上，使用hadoop jar wordCut.jar /data/task2/novels /data/task2/people_name_list.txt out
 *
 */
public class WordCut {
	
	//Map阶段
    public static class WordCutMapper extends Mapper<Object, Text, Text, Text>{
    	
        public static Text nullText;
        public static Set<String>  library = new HashSet<>();
        
        //初始化
        public void setup(Context context){
            nullText = new Text("");
            try {
            	//读取人名列表，该人名列表在main方法中已经加载到distributed Cache中
            	@SuppressWarnings("deprecation")
				Path[] localFiles = context.getLocalCacheFiles();
            	FileReader fr = new FileReader(localFiles[0].toString());
                BufferedReader br = new BufferedReader(fr);
                
                //按行读取人名列表，加入到library中
                String text = new String();
                while((text = br.readLine()) != null){
                    library.add(text);
                }
                br.close();
            }catch (IOException e) {
                System.err.println("Exception when reading DistributedCache:" + e);
            }
            /*
             * 因为小说是原本小说，没有分词，所以借助了外部工具ansj_seg
             * 下面的循环将library中的人名一个个加入到ansj_seg的用户自定义词典中，属性为"userDefine"
             */
            for(String str:library){
                UserDefineLibrary.insertWord(str,"userDefine",1000);
            }
        }
        //map方法
        public void map(Object key,Text value,Context context)throws IOException,InterruptedException {
        	StringBuffer str = new StringBuffer();
            List<Term> names = DicAnalysis.parse(value.toString()).getTerms();
            for(Term name : names) {
                if(name.getNatureStr().equals("userDefine")){	
            		str.append(name.getName());
                    str.append(" ");
                }
            }
            //如果str不为空，即这段话至少含有一个人名
            if(str.length() != 0){
            	 //将两个不同的人关联起来，加入到hash表中
            	 //格式为：胡斐#戚芳
            	Set<String>  result = new HashSet<>();
            	String[] tmp = str.toString().split(" ");
            	for(int i = 0; i < tmp.length; i++){
            		for(int j = 0; j < tmp.length; j++){
            			if(tmp[i].equals(tmp[j]) == false){ //排除掉 胡斐#胡斐的情况
            				//result是HashSet，只有在A#B不存在的情况下才会加进去确保同一段中每一对人物共现次数被统计为1
            				result.add(tmp[i] + "#" + tmp[j]);
            			}
            		}
            	}
            	for(String t : result){
            		String[] res = t.split("#");
            		context.write(new Text(res[0]), new Text(res[1]));
            	}
            }
        }
    }
    //reduce阶段
    public static class WordCutReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key,Iterable<Text>values,Context context) throws IOException,InterruptedException{
        	/*
        	 * 定义了一个HashMap，用于存储人物共现的次数
        	 * count用于统计与A关联的总人次
        	 * nameList用于存储与A关联的所有人名
        	 */
        	int count = 0;
        	HashMap<String, Integer> map = new HashMap<String, Integer>();
        	Set<String>  nameList = new HashSet<>();
        	
        	for(Text value : values){
        		count++;
        		String name = value.toString();
        		nameList.add(name);
        		/*
        		 * 如果A和B之前共现过了，那么共现次数加1
        		 * 如果没共现过，那么共现次数为1
        		 */
        		if(map.containsKey(name)) map.put(name, map.get(name) + 1);
        		else map.put(name, 1);
        	}
        	/*
        	 * 遍历与A共现的人名列表，map.get(B)获得A与B共现的次数，除以count，得到B在A的关系网里的比重，至此任务三就完成了
        	 */
        	StringBuffer res = new StringBuffer();
        	for(String name : nameList){
        		double p = (double)map.get(name) / count;
        		res.append(name + "," + p);
        		res.append(" | ");
        	}
        	context.write(key, new Text(res.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        
        Configuration conf=new Configuration();
        String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
        /*
         * 参数有三个，依次为：小说路径、人名列表路径、输出路径
         */
        if (otherArgs.length!=3){
            System.err.println("Argument Error!");
            System.exit(2);
        }
        
        @SuppressWarnings("deprecation")
		Job job=new Job();
        job.addCacheFile(new Path(args[1]).toUri());//人名列表作为分布式cache共享到每一个节点
        job.setJarByClass(WordCut.class);
        job.setMapperClass(WordCutMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(WordCutReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path outputPath = new Path(otherArgs[2]);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        outputPath.getFileSystem(conf).delete(outputPath,true);
        System.exit(job.waitForCompletion(true)?0:1);
    }

}

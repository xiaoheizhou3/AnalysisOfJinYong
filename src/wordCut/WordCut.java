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

public class WordCut {

    public static class WordCutMapper extends Mapper<Object, Text, Text, Text>{
//        public static HashSet<String> library;
        public static Text nullText;
        public static Set<String>  library = new HashSet<>();
        
        public void setup(Context context){
 //           library = new HashSet<>();
            nullText = new Text("");
            
            try {
            	@SuppressWarnings("deprecation")
				Path[] localFiles = context.getLocalCacheFiles();
            	FileReader fr = new FileReader(localFiles[0].toString());
                BufferedReader br = new BufferedReader(fr);
                String text = br.readLine();
                while(text != null){
                    library.add(text);
                    text = br.readLine();
                }
                br.close();
            }catch (IOException e) {
                System.err.println("Exception reading DistributedCache:" + e);
            }
            for(String str:library){
                UserDefineLibrary.insertWord(str,"userDefine",1000);
            }
        }

        public void map(Object key,Text value,Context context)throws IOException,InterruptedException {
            StringBuffer str = new StringBuffer();
            List<Term> names = DicAnalysis.parse(value.toString()).getTerms();
            for(Term t:names) {
                //if(library.contains(t.getName())){
                if(t.getNatureStr().equals("userDefine")){	
            		str.append(t.getName());
                    str.append(" ");
                }
            }
            if(str.length() != 0){
            	//任务一分词的输出
            	/*
            	Text text = new Text(str.toString());
            	context.write(text,nullText);
            	*/
            	//任务二统计人物关系
            	Set<String>  result = new HashSet<>();
            	String[] tmp = str.toString().split(" ");
            	for(int i = 0; i < tmp.length; i++){
            		for(int j = 0; j < tmp.length; j++){
            			if(tmp[i].equals(tmp[j]) == false){
            				result.add(tmp[i] + "#" + tmp[j]);
            			}
            		}
            	}
            	for(String t : result){
            		//任务二的代码
            		/*
            		Text text = new Text(t);
            		context.write(text, nullText);
            		*/
            		//任务三的代码
            		String[] res = t.split("#");
            		context.write(new Text(res[0]), new Text(res[1]));
            	}
            }
        }
    }

    public static class WordCutReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key,Iterable<Text>values,Context context) throws IOException,InterruptedException{
        	//任务二的reduce，统计共现次数
        	/*
        	int count = 0;
        	for(Text value : values){
        		count++;
        		value.set("1");//没用的，只是为了不报value未使用的警告
        	}
            context.write(key,new Text(" " + count));
            */
        	//任务三的reduce，统计概率
        	int count = 0;
        	HashMap<String, Integer> map = new HashMap<String, Integer>();
        	Set<String>  nameList = new HashSet<>();
        	for(Text name : values){
        		count++;
        		String tmp = name.toString();
        		nameList.add(tmp);
        		if(map.containsKey(tmp)) map.put(tmp, map.get(tmp) + 1);
        		else map.put(tmp, 1);
        	}
        	StringBuffer res = new StringBuffer();
        	for(String name : nameList){
        		float p = (float)map.get(name) / count;
        		res.append(name + p);
        		res.append(" | ");
        	}
        	context.write(key, new Text(res.toString()));
        }
    }

    public static void deleteDir(File file) throws FileNotFoundException {
        if(!file.exists())
            throw new FileNotFoundException();
        if(file.isDirectory()) {
            File[] files = file.listFiles();
            for (File f : files)
                deleteDir(f);
        }
    }

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        if(args.length != 3){
            System.out.println("args not match!");
            System.exit(-1);
        }

        File dir = new File(args[1]);
        if(dir.exists())
            deleteDir(dir);
        
        Configuration conf=new Configuration();
        String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
        
        if (otherArgs.length!=3){
            System.err.println("Usage:PreProcess<in><out>");
            System.exit(2);
        }
        
        @SuppressWarnings("deprecation")
		Job job=new Job();
        job.addCacheFile(new Path(args[1]).toUri());
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
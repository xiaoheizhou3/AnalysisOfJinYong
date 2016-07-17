package csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class ExportCSV {

    /**
     * 创建CSV文件
     * @throws IOException 
     */
	public static void main(String[] args) throws IOException{
		createCSV();
	}
    public static void createCSV() throws IOException {
    	
        String fileName = "Node.csv";//文件名称
        String filePath = "H:/Test/"; //文件路径
        
        // 表格头
        Object[] head = { "ID", "Label", "Class","PageRank" };
        List<Object> headList = Arrays.asList(head);

        //读取人名rank值文件
        File nameRankFile = new File("H://nameRank");
        InputStreamReader nrf = new InputStreamReader (new FileInputStream(nameRankFile),"UTF-8");   
        BufferedReader nbr = new BufferedReader(nrf);
        HashMap<String,String> map = new HashMap<>();
        String nameRank = new String();
        while((nameRank = nbr.readLine()) != null){
        	String[] tmp = nameRank.split("\t");
        	map.put(tmp[0], tmp[1]);
        }
        nbr.close();
        //数据
        List<List<Object>> dataList = new ArrayList<List<Object>>();
        List<Object> rowList = null;
        File f = new File("H://result");
        InputStreamReader fr = new InputStreamReader (new FileInputStream(f),"UTF-8");   
        BufferedReader br = new BufferedReader(fr);
        String text = new String();
        while((text = br.readLine()) != null){
        	String[] tmp = text.split("\t");
        	if(tmp.length >= 2){
        		rowList = new ArrayList<Object>();
        		String idLabel = tmp[0].trim();
        		rowList.add(idLabel);
        		rowList.add(idLabel);
        		String myClass = tmp[1].trim();
        		rowList.add(myClass.substring(1, tmp[1].length() - 1));
        		rowList.add(map.get(idLabel));
        		dataList.add(rowList);
        	}
        }
        br.close();
        
        File csvFile = null;
        BufferedWriter csvWtriter = null;
        try {
            csvFile = new File(filePath + fileName);
            File parent = csvFile.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }
            csvFile.createNewFile();

            csvWtriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFile), "GBK"), 1024);
                       
            int num = headList.size() / 2;
            StringBuffer buffer = new StringBuffer();
            for (int i = 0; i < num; i++) {
                buffer.append(" ,");
            }
            csvWtriter.write(buffer.toString() + fileName + buffer.toString());
            csvWtriter.newLine();

            // 写入文件头部
            writeRow(headList, csvWtriter);

            // 写入文件内容
            for (List<Object> row : dataList) {
                writeRow(row, csvWtriter);
            }
            csvWtriter.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                csvWtriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    /**
     * 写一行数据
     * @param row 数据列表
     * @param csvWriter
     * @throws IOException
     */
    private static void writeRow(List<Object> row, BufferedWriter csvWriter) throws IOException {
        for (Object data : row) {
            StringBuffer sb = new StringBuffer();
            String rowStr = sb.append("\"").append(data).append("\",").toString();
            csvWriter.write(rowStr);
        }
        csvWriter.newLine();
    }
}

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
import java.util.List;

public class ExportCSV2 {

    /**
     * ����CSV�ļ�
     * @throws IOException 
     */
	public static void main(String[] args) throws IOException{
		createCSV();
	}
    public static void createCSV() throws IOException {
    	
        String fileName = "Edge.csv";//�ļ�����
        String filePath = "H:/Test/"; //�ļ�·��
        
        // ����ͷ
        Object[] head = { "Source", "Target" };
        List<Object> headList = Arrays.asList(head);

        //����
        List<List<Object>> dataList = new ArrayList<List<Object>>();
        List<Object> rowList = null;
        File f = new File("H://relation");   
        InputStreamReader fr = new InputStreamReader (new FileInputStream(f),"UTF-8");   
        BufferedReader br = new BufferedReader(fr);
        String text = new String();
        while((text = br.readLine()) != null){
        	String[] tmp = text.split("\t");
        	if(tmp.length >= 2){
        		String[] nameList = tmp[1].split(" | ");
        		for(int i = 0; i < nameList.length; i++){
        			String[] name = nameList[i].split(",");
        			if(name.length >= 2){
                		rowList = new ArrayList<Object>();
                		rowList.add(tmp[0]);
                		rowList.add(name[0]);
                		dataList.add(rowList);
        			}
        		}
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

            // д���ļ�ͷ��
            writeRow(headList, csvWtriter);

            // д���ļ�����
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
     * дһ������
     * @param row �����б�
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
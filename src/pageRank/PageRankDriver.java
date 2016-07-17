package pageRank;

public class PageRankDriver {
	private static int times = 20;
	public static void main(String[] args) throws Exception{
		//pagerank预处理
		String[] forItr = { "", ""};
		forItr[0] = args[0];
		forItr[1] = args[1] + "/Data0";
		PrePageRank.main(forItr);
		
		//pagerank算法，迭代times次
		String[] pagerank = { "", ""};
		for(int i = 0; i < times; i++){
			pagerank[0] = args[1] + "/Data" + i;
			pagerank[1] = args[1] + "/Data" + String.valueOf(i + 1);
			PageRank.main(pagerank);
		}
		//对pagerank结果从小到大排序
		String[] forRv = { args[1] + "/Data" + times, args[1] + "/FinalRank" };
		Sort.main(forRv);
	}
}

## 金庸小说人物关系分析——基于MapReduce

使用hadoop MapReduce，提取出金庸小说中出现的所有人名，并统计每两个人名在同一段落中是否共现，根据共现的次数，计算出人物B在人物A的关系网中的权重,使用PageRank算法计算出每一个角色的PageRank权值，再进行排序，根据排序结果就可以直观地看出，谁是金庸小说里的主角，谁是配角，谁是小角色。

###运行方法

在hadoop集群上，运行wordCut程序的Jar包，运行的参数分别为：金庸小说全集、金庸小说人名列表、输出路径（假设为out）

接着运行pageRank程序的Jar包，运行的参数分别为：out（即wordCut的结果）、输出路径（假设为out_pageRank）

接着运行Sort程序的Jar包，运行的参数分别为：out_pageRank、输出路径（假设为out_Sort）

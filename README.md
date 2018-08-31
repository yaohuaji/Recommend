基于物品的商品推荐

工程执行顺序：
com.yhj.duplicate.removal:数据清洗，主要清洗掉重复的数据
com.yhj.userlike:获取用户的喜爱矩阵，即用户对商品的评价分数
com.yhj.tongxian:获取商品的同显次数
com.yhj.multi.tongxian:商品同显矩阵与用户喜爱矩阵相乘
com.yhj.sum:求和，得到每一个用户的所有商品推荐分数
com.yhj.sort:排序，将每一给用户的推荐分数的Top10推荐给用户


工程只有一个Runner类，RunnerTest用来测试每一个MapReduce。
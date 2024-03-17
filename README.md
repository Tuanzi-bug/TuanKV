# KV存储

## 相关流程记录

1. 启动bitcask引擎实例
   1. 对配置项进行校验
      1. 校验地址是否为空
      2. 数据文件阈值无效
   2. 对数据目录进行校验，不存在则创建
   3. 初始化DB结构体
      1. 对索引进行初始化
      2. 配置项添加索引类型

2. 加载数据目录中的文件
   1. 读取目录文件
   2. 遍历文件，找到目标文件，搜索条件以`.data`结尾
      1. 找到文件，对文件名进行分割，获取文件ID
      2. 处理错误，并存放ID
   3. 文件ID进行排序
   4. 遍历文件ID并打开对应数据文件
      1. 最后一个ID放置于活跃文件中
      2. 其余放置于旧的文件中
3. 数据文件中加载索引
   1. 获取文件ID
   2. 找到对应文件
   3. 循环处理文件内容，读取数据
      1. 构建内存索引
      2. 判断记录类型，保持进入索引中
      3. 更新偏移值
   4. 判断当前活跃文件，需要更新当前文件偏移值



1. 数据删除流程
   1. 判断key的有效性
   2. 从内存索引中查找key是否存在，不存在直接返回
   3. 构造数据记录信息，写入数据文件中
   4. 从内存索引中将对应key进行删除



## 相关参考

* https://codecapsule.com/2012/11/07/ikvs-implementing-a-key-value-store-table-of-contents/

* https://www.cnblogs.com/whuanle/p/16297025.html

* https://github.com/nutsdb/nutsdb



## 其他

#### git commit 规范参考

commit message格式

><type>(<scope>): <subject>


![image](./assets/162549766-58f164df-3794-4a5a-ab25-dd47962de74e.png)

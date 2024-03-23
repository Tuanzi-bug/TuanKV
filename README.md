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



1. IO管理器
   1. 在IOManager的接口中新增一个打开对应IO的方法- NewIOManager
   2. 目前先简单处理，直接调用NewFileIOManager即可，后续需要新增在进行判断
2. 打开数据文件 OpenDataFile
   1. 根据path和Id生成一个完整的文件名称
   2. 初始化IOManager，直接返回错误
   3. 初始化DataFile结构体，并返回
4. 读取LogRecord
   1. 设置头部size为固定值：maxLogRecordHeaderSize
   2. 封装一个读取指定字节数的函数 readNBytes
   3. 读取头部信息，拿到对应信息
   4. 写一个解码相关函数 decodeLogRecordHeader，以及一个头部信息的结构体 logRecordHeader
   5. 两个条件，说明读取到了文件末尾，返回EOF错误
   6. 取出key和Value的长度
   7. 读取实际的用户存储的Key和value数据
   8. 校验crc是否正确，定义新的方法getlogRecordCRC拿到crc的值
   9. 如果不相等需要返回错误，定义错误类型 ErrInvalCRC
10. 写write方法需要更新字段
11. 特殊处理
    1. 在ioManager添加一个获取文件大小方法
    2. 需要重新添加一个判断，如果读取长度大于文件长度，只需要读取到实际长度
3. 写相关单元测试
   1. 打开文件
   2. 写文件



1. LogRecord编解码
   1. 初始化一个header部分字节数组，创建一个header数组
   2. 从第五个字节开始存储 Type类型
   3. 从后面开始存放后面的数据 key和value的长度信息
   4. 返回一条信息实际大小
   5. 重新创建一个数组
   6. 将header部分的内容拷贝过来，将数据key和value一起拷贝过去
   7. 生成crc，除了前面4个字节的
   8. 使用小端序进行存储信息，并返回信息与长度

2. 解码的流程
   1. 首先判断长度是否满足4字节，直接返回
   2. 构建一个logRecordHeader结构体
   3. 解码key和value的长度信息
   4. 返回结构体和长度信息

3. getLogRecordCRC
   1. 首先进行编码header信息
   2. 对key和value数据信息进行更新写入
   3. 返回crc值
4. 添加单元测试
   1. 编码测试
      1. 正常情况
      2. value为空情况
      3. delete删除情况下
   2. 解码测试
   3. 

   

1. 定义索引迭代器接口
   1. 定义btree树索引迭代器
      1. 当前下标 currIndex
      2. reverse 反向遍历标志
      3. value 存放key+位置索引信息
   2. 实现接口的每一个方法
   3. 实现创建btree索引迭代器的函数
   4. 在Index的接口添加一个返回迭代器的一个方法 Iterator
   5. 增加单元测试


1. 对外的迭代器
   1. 设置迭代器配置项
   2. 定义默认索引迭代器
   3. 面向用户方法与内部一致
      1. 对Value方法需要返回对应的值
      2. 根据get方法进行简化，根据位置信息获取value值
   4. 需要增加判断是否满足前缀要求
   5. 获取数据库所有的key--ListKeys
   6. 索引新增一个Size方法，方便获取长度
   7. 对数据库信息执行指定操作Fold 
   8. 不全数据库close与sync方法
   9. close：关闭当前活跃文件以及遍历并关闭旧的文件，该方法需要加锁
   10. sync：对当前活跃文件进行持久化
## 相关参考

* https://codecapsule.com/2012/11/07/ikvs-implementing-a-key-value-store-table-of-contents/

* https://www.cnblogs.com/whuanle/p/16297025.html

* https://github.com/nutsdb/nutsdb



## 其他

#### git commit 规范参考

commit message格式

><type>(<scope>): <subject>


![image](./assets/162549766-58f164df-3794-4a5a-ab25-dd47962de74e.png)

# estuary
基于Akka实现的高性能数据实时流式同步的应用


hi,大家好 失踪人口回归 

`estuary`其实一直都没有停,但是闭源版和业务耦合太深，没法直接放出来，我这次准备慢慢整理出开源版，这次直接放出Mysql2Mysql,之后很快就会放出Mongo,Kafka等Source或者Sink
虽然闭源版的功能已经稳定，但是开源版还是需要测试验证，我会一点点补充测试用例的，绝不弃坑


> 注:以后专注维护3.x

---

### 简介

 estuary致力于提供一个数据实时同步的方案,实现从数据源到数据汇的端到端实时同步，旨在解决异构数据同步问题
### 架构与设计
#### 数据的生命周期
数据在整个程序运行过程中分为*三*个阶段
 - 从数据源获取数据,称之为source
 - 中间处理装换部分,称之为transform
 - 数据写入数据汇，称之为sink

顺序如下图
> `SOURCE`->`TRANSFORM`->`SINK`


#### 技术选型

本程序以Akka为核心构建起来的，利用Akka驱动程序的逻辑流程
目前完成了Mysql Binlog 到 Mysql的实现
- 编程语言:Scala(主)/Java
- 并发框架及主逻辑串联：Akka
- Binlog解析:Canal(将canal解析部分源码提出并使用)
- 日志：Akka logging + slf4j
- Restful服务:Spring Boot
- Json序列化/反序列化:Jackson
- 数据库连接池 HikariCP
- ANTLR4 SQL PARSER(specially thanks to [maxwell](https://github.com/zendesk/maxwell]))

#### 功能域划分
 功能域目前拆解成两个大部分，分别是`同步域`和`元数据管理域` 
 
##### 同步域的设计
同步域是整个程序的核心
<br>同步域是由若干个功能各不相同的`worker`组合而成，worker对应的具体实现就是一个/一组Actor</br>
<br>
每个worker都有其角色
  - syncController 同步任务的控制器 负责初始化其他部件和发送一些控制语句
  - fetcher 负责拉取数据发送给batcher
  - batcher 负责将数据进行处理并打包
  - sinker  负责写入数据
  - listener 心跳监听
  - syncDaemon 同步域的守护Actor，只会有一个
  - positionRecorder 记录位置
  - powerAdapter 回压组件
  - processingCounter 计数器
</br>
  
  SyncContoller是整个同步任务的发起者，是其他(除了SyncDaemon以外的)Actor的父Actor
  
数据/消息的流向如所示  
![image](https://github.com/shouweikun/estuary/raw/master/img/dataflow.png)

---

##### 数据的事务分区等级与实现逻辑
对于整个同步流程来说，事务的等级与同步的效率成负相关
###### 等级
 - MOD 完全的自由分配,roundRobin,不保证投递顺序
 - PRIMARY_KEY 保证**ID级有序**,保证事务的最终一致性
 - DATABASE_TABLE 保证库表级的顺序,保证事务的最终一致性
 - TRANSACTION 完成保证事务的一致性

###### 性能
  `MOD` >= `PRIMARY_KEY` >= `DATABSE_TABLE` >> `TRANSACTION`
  
###### 逻辑

主要在三个阶段实现:

 -  库表级数据分离
 -  主键/ID级数据分离
 -  SINK数据分离



![image](https://github.com/shouweikun/estuary/raw/master/img/transaction_rate.png)
 

---

##### 功率控制(反压)的实现
 
Estuary自带了功率控制功能，避免V<sub>source</sub> > V<sub>sink</sub>产生问题

###### 步骤
 1. 开启Estuary的性能数据收集(默认开启)
 2. 根据收集的性能参数计算fetchDelay
 3. set fetchDelay


##### 元数据管理域的设计
对于有schema信息的数据源，处理复杂schema变化情况下的相应schema变化


schema读取和更新的三个层次
1. 内存中缓存元数据信息
2. 元数据数据库中存储的元数据信息
3. (其他/最终)SINK中应对元数据相应操作

元数据管理的生命周期
1. 程序初始化的元数据加载和缓存
2. 程序运行时的元数据读取与校验
3. 程序运行时的元数据变更与刷新 



### 1.x 
支持简单Mysql2Kafka
现已不维护

### 2.x
支持Mysql2kafka
支持元数据管理域
现已不维护
 
```json
{
	"batchThreshold": 1, //打包大小 默认1
	 "binlogJournalName":"mysql-bin.005539", //可不填 默认从最后
	 "binlogPosition":4, //可不填
	"costing": true,
	"counting": true,
	"defaultConnectionTimeoutInSeconds": "3000",
	"fetchDelay": 0,
	"filterBlackPattern": "",
	"filterPattern": "xxx\\..*",
	"filterQueryDcl": false,
	"filterQueryDdl": false,  
	"filterQueryDml": false,
	"filterRows": false,
	"filterTableError": false,
	"kafkaAck": "1",
	"kafkaBootstrapServers": "xxxx" //必填,
	"kafkaLingerMs": "0",
	"kafkaMaxBlockMs": "2000",
	"kafkaRetries": "3",
	"kafkaSpecficTopics": {},
	"kafkaDdlTopic": "SrcMyDdl",
	"kafkaTopic": "estuary1",
	"listenRetrytime": 3,
	"listenTimeout": 5000,
	"powerAdapted": true,
	"profiling": true,
	"receiveBufferSize": 0,
	"sendBufferSize": 0,
	"syncTaskId": "xxxx",//必须填
	"sync":false,
	"taskType": 2,
	"zookeeperServers": "", //必须填
	"zookeeperTimeout": 20000
}

```
### 3.x
现在专注维护3.x

#### 已经实现的功能
 - 目前提供mysql 到 mysql 的数据同步实现
 - **支持ddl的实时更新sink mysql的功能,支持定制选项** (重要)
 - 基于Akka框架，在高可用(HA)方面做了很多工作，应对所有notFatal级错误
 - 遵循着"Let it crash"理念对异常的处理很轻松
 - 利用spring 提供了简易的restful接口
 - 实现了任务的开始停止重启生命周期相关功能
 - 实现了查看同步详情，包括：在拉取数据(fetch)，处理打包（batch),沉降(sink)的performance记录，Count记录
 - 实现了较为完备自动功率控制(反压)
 - 实现的在同步任务的每一个流程的细粒度定制
 - 实现了应对复杂schema信息变化的元数据管理(2.x支持，3.x还需要做一些适配调整)
 - 完备的代码抽象
 - 动态组件指定(再启动任务时灵活指定组件)


#### todo
1. 更多样的source和sink
2. 分布式精准快照
3. 日志追踪功能
4. 任务信息持久化
5. 去Spring化（可能采用Akka http 或者 Play!）
6. 打包机制
7. 集群化
8. 双写备份和精准快照恢复

#### 使用
 > 在这假定你使用Idea进行开发
```j
 将ANTLR的文件夹指定为source folder
 mvn compile
```
 ```
 cp application.properties.templete  application.properties
 cp application.conf.templete  application.conf 
编辑文件来配置你的属性
```
```
mvn package
```

 ```
./bin/start
```
```
调用接口，详情产看Swagger-ui.html
```
##### 启动接口详细信息  POST /api/v1/estuary/mysql2mysql/new/sync
```json
{
  "mysql2MysqlRunningInfoBean": {
    "batchThreshold": 1,
    "batcherNameToLoad": {}, //选填 batcher动态加载
    "batcherNum": 23,//不能小于1
    "controllerNameToLoad": {},//选填，controller动态加载
    "costing": true, //是否计数
    "counting": true, //是否计算耗时
    "fetcherNameToLoad": {}, //选填 fetcher动态加载
    "mappingFormatName":"string",//选择加载的处理模式
    "mysqlDatabaseNameList": [
      "string"
    ], //选填，数据库名称
    "needExecuteDDL": true, //是否执行ddl
    "offsetZkServers": "string", //必填，zk地址
    "partitionStrategy": "PRIMARY_KEY",// 分区策略
    "powerAdapted": true,//是否功率调节
    "profiling": true, //是否计算详细信息
    "schemaComponentIsOn": true, //是否开启元数据管理模块
    "sinkerNameToLoad": {},//选填 sinker 动态加载
    "startPosition": {  //可选
      "included": true,
      "journalName": "string",
      "position": 0,
      "serverId": 0,
      "timestamp": 0
    },
    "syncStartTime": 0, //同步开始时间
    "syncTaskId": "string" //必填，任务id
  },
  "mysqlSinkBean": {
    "autoCommit": true,  //是否自动提交
    "connectionTimeout": 0, //选填 time时间
     "credential": {  //必填
      "address": "string",
      "defaultDatabase": "string",
      "password": "string",
      "port": 0,
      "username": "string"
    },
    "maximumPoolSize": 0 //选填
  },
  "mysqlSourceBean": {
    "concernedDatabase": [  //必填
      "string" 
    ],
    "filterBlackPattern": "string", //选填，过滤
    "filterPattern": "string", //选填，白名单
    "filterQueryDcl": true,  //选填
    "filterQueryDdl": true,  //..
    "filterQueryDml": true,//..
    "filterRows": true,//..
    "filterTableError": true,//..
    "ignoredDatabase": [
      "string"
    ],
   "master": {  //必填
      "address": "string",
      "defaultDatabase": "string",
      "password": "string",
      "port": 0,
      "username": "string"
    }
  }
}
```
```json
//一个样例

{
	"mysql2MysqlRunningInfoBean": {
		"batcherNum": 31,
		"offsetZkServers": "nbhd.aka.laplace.zookeeper.com:2181",
		"partitionStrategy": "PRIMARY_KEY",
		"syncTaskId": "nbhd",
		"sinkerNameToLoad": {
			"sinker": "com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.MysqlBinlogInOrderMysqlRingBufferSinker" //推荐使用这个
		},
		"startPosition": {
			"timestamp": 1548126793000 //binlog会从这个时间点消费
		}
	},
	"mysqlSinkBean": {
		"credential": {
			"address": "localhost",
			"defaultDatabase": "",
			"password": "123456",
			"port": 3306,
			"username": "root"
		}
	},
	"mysqlSourceBean": {
		"concernedDatabase": [
			"xxx"
		],
		"filterPattern": "xxx\\.yyy", //白名单过滤
		"master": {
			"address": "localhost",
			"defaultDatabase": "",
			"password": "123456",
			"port": 3306,
			"username": "root"
		}
	}

}
```
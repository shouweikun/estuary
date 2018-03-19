# estuary
基于Akka实现的数据实时流式同步的应用

---
### 简介

estuary致力于提供一个数据实时同步的方案,实现从source到sink的端到端实时同步

### 架构

一个同步任务由若干个`worker`组成
<br>每个worker都有其角色
  - syncController 同步任务的控制器 负责初始化其他部件和发送一些控制语句
  - fetcher 负责拉取数据发送给batcher
  - batcher 负责将数据进行处理并打包
  - sinker  负责写入数据
  - listener 心跳监听
  
</br>
遵循着"Let it crash"理念对异常的处理很轻松


### 已经实现的功能
 - 目前提供mysql 到kafka 的数据同步实现
 - 基于Akka框架，在高可用(HA)方面做了很多工作，应对所有notFatal级错误
 - 利用spring 提供了简易的restful接口
 - 实现了任务的开始停止重启功能
 - 实现了查看同步详情，包括：在拉取数据(fetch)，处理打包（batch),沉降（sink)的performance记录，Count记录
 - 实现了简单的自动功率控制
 - 实现的在同步任务的每一个流程的细粒度定制


### todo
1. 更多样的source和sink
2. 更加完备的代码抽象
3. 日志追踪功能
4. 任务信息持久化
5. 去Spring化（可能采用Akka http 或者 Play!）
6. 打包机制
7. 反压
8. 集群化

 


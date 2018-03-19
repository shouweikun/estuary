# estuary
基于Akka实现的数据实时流式同步的应用

---
### 简介

estuary致力于提供一个数据实时同步的方案,实现从source到sink的端到端实时同步

### 已经实现的功能
 - 目前提供mysql 到kafka 的数据同步实现
 - 基于Akka框架，在高可用(HA)方面做了很多工作，应对所有notFatal级错误
 - 利用spring 提供了简易的restful接口
 - 实现了任务的开始停止重启功能
 - 实现了查看同步详情，包括：在拉取数据(fetch)，处理打包（batch),沉降（sink)的performance记录，Count记录
 - 实现了简单的自动功率控制


### todo
1. 更多样的source和sink
2. 更加完备的代码抽象
3. 日志追踪功能
4. 任务信息持久化
5. 去Spring化（可能采用Akka http 或者 Play!）
6. 打包机制
 


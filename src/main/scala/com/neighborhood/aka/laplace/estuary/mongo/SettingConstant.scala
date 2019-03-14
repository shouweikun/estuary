package com.neighborhood.aka.laplace.estuary.mongo

/**
  * Created by john_liu on 2018/4/12.
  * 单位秒
  */
object SettingConstant {

  val COMPUTE_COST_CONSTANT = 3
  val COMPUTE_COUNT_CONSTANT = 100 //这个单位是ms
  val COMPUTE_FIRST_DELAY: Int = 5
  val OFFSET_SAVE_CONSTANT: Int = 60
  val CHECKSEND_CONSTANT = 20
  val BATCHER_START_DELAY = 1
  val FETCHER_START_DELAY = 5
  val POWER_CONTROL_CONSTANT = 5 //这个单位是ms
  val LISTEN_QUERY_TIMEOUT = 5
  val TASK_RESTART_DELAY = 10
  val HASH_MAPPING_VIRTUAL_NODES_FACTOR = 32
  val CHECK_STATUS_INTERVAL = 20
  val SINKER_FLUSH_INTERVAL = 300 //这个单位是毫秒
  val FAILURE_RETRY_BACKOFF = 100 //ms
  val CHECK_ACTIVE_INTERVAL = 30 //s
}

package com.neighborhood.aka.laplce.estuary.web.controller

import com.neighborhood.aka.laplce.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplce.estuary.web.bean.Mysql2kafkaTaskRequestBean
import com.neighborhood.aka.laplce.estuary.web.service.Mysql2KafkaService
import io.swagger.annotations.{Api, ApiOperation}
import org.springframework.web.bind.annotation.{RequestBody, RequestMapping, RequestMethod, RestController}

/**
  * Created by john_liu on 2018/3/10.
  */
@RestController
@RequestMapping(Array("/api/v1/estuary"))
class Mysql2KafkaTaskController {


//    @GetMapping(Array(""))
//    def getAllSparkJobEntity() = {
//      val all = sparkEntityDao.findAll().toList.map(_.toView())
//      JsonHelper.to(all)
//    }
@ApiOperation(value = "1", httpMethod = "POST", notes = "1 ")
@RequestMapping(value = Array("/newTask/"), method = Array(RequestMethod.POST))
  def createNewTask(@RequestBody requestBody: Mysql2kafkaTaskRequestBean) = {
      //todo 检验任务合法性
       println("1")
    }

}

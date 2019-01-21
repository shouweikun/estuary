package com.neighborhood.aka.laplace.estuary.mysql.source

import com.neighborhood.aka.laplace.estuary.bean.credential.MysqlCredentialBean
import com.neighborhood.aka.laplace.estuary.bean.resource.MysqlSourceBean

/**
  * Created by john_liu on 2019/1/13.
  * Mysql数据源构建需要的信息Bean
  *
  * @author neighborhood.aka.laplace
  */
final case class MysqlSourceBeanImp(
                                     override val master: MysqlCredentialBean,
                                     override val concernedDatabase: List[String],
                                     override val ignoredDatabase: List[String],
                                     override val filterPattern: Option[String] = None,
                                     override val filterBlackPattern: Option[String] = None
                                   )(
                                     override val filterQueryDcl: Boolean = false,
                                     override val filterQueryDml: Boolean = false,
                                     override val filterQueryDdl: Boolean = false,
                                     override val filterRows: Boolean = false,
                                     override val filterTableError: Boolean = true //默认开启
                                   ) extends MysqlSourceBean



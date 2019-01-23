package com.neighborhood.aka.laplace.estuary

import org.scalamock.scalatest.MockFactory
import org.scalatest._

abstract class UnitSpec extends FlatSpec with MockFactory with Matchers with
  OptionValues with Inside with Inspectors
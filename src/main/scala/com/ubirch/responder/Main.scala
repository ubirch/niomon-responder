package com.ubirch.responder

import com.ubirch.niomon.base.NioMicroserviceLive
import ResponderMicroservice._

object Main {
  def main(args: Array[String]): Unit = {
    val _ = NioMicroserviceLive("responder", ResponderMicroservice(_)).runUntilDoneAndShutdownProcess
  }
}

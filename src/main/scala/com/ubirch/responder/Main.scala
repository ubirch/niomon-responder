package com.ubirch.responder

object Main {
  def main(args: Array[String]): Unit = {
    val _ = new ResponderMicroservice().runUntilDoneAndShutdownProcess
  }
}

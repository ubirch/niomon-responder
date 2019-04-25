package com.ubirch.responder

object Main {
  def main(args: Array[String]): Unit = {
    new ResponderMicroservice().runUntilDoneAndShutdownProcess
  }
}

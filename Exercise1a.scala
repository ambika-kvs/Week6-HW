package Week6

import scala.math.random

object Exercise1a {
  def main(args: Array[String]): Unit = {
    var count = 0
    for (i <- 1 to 100000) {
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y <= 1) count += 1
    }
    println(s"Pi is ${4 * count / 100000}")
  }
}
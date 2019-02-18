import java.util

import lombok.Data

import scala.collection.immutable.HashMap

/**
  * @Description:
  *
  * <p></p>
  * @author jsen.yin [jsen.yin@gmail.com]
  *         2019-02-18
  */

@Data
case class WorkerInfo(id: String, cpuUsage: Double) {

  def getCpuUsage(): Double = {
    return this.cpuUsage
  }

  def getId(): String = {
    return this.id
  }

}


object Test {

  def main(args: Array[String]): Unit = {

    var sortHash = new HashMap[String, WorkerInfo]

    sortHash += (
      "1" -> WorkerInfo("a", 0.4),
      "2" -> WorkerInfo("b", 0.2),
      "3" -> WorkerInfo("c", 0.3)
    )

    sortHash.toList.sortBy(_._2.cpuUsage) foreach {
      case (key, value) => println(key + "==" + value.getId() + "==" + value.getCpuUsage())
    }


  }

}

package pipelinepack

import consumerpack.QueryStatic

object Pipeline4HivetableToHivequery {
  def main(args: Array[String]) = {
    println("Pipeline4HivetableToHivequery started...")

    QueryStatic.oldMain()

  }
}
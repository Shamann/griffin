/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.griffin.measure

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter}
import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import org.apache.commons.io.FileUtils

import org.apache.griffin.measure.configuration.dqdefinition.{
  DQConfig,
  EnvConfig,
  GriffinConfig,
  Param
}
import org.apache.griffin.measure.configuration.dqdefinition.reader.ParamReaderFactory
import org.apache.griffin.measure.configuration.enums.ProcessType
import org.apache.griffin.measure.configuration.enums.ProcessType._
import org.apache.griffin.measure.launch.DQApp
import org.apache.griffin.measure.launch.batch.BatchDQApp
import org.apache.griffin.measure.launch.streaming.StreamingDQApp

/**
 * application entrance
 */
object Application extends Loggable {

  def main(args: Array[String]): Unit = {
    info(args.toString)
    if (args.length < 2) {
      error("Usage: class <env-param> <dq-param>")
      sys.exit(-1)
    }

    val envParamFile = args(0)
    val dqParamFile = args(1)

    info(envParamFile)
    info(dqParamFile)

    // read param files
    val envParam = readParamFile[EnvConfig](envParamFile) match {
      case Success(p) => p
      case Failure(ex) =>
        error(ex.getMessage, ex)
        sys.exit(-2)
    }
    val dqParam = readParamFile[DQConfig](dqParamFile) match {
      case Success(p) => p
      case Failure(ex) =>
        error(ex.getMessage, ex)
        sys.exit(-2)
    }
    val allParam: GriffinConfig = GriffinConfig(envParam, dqParam)

    // choose process
    val procType = ProcessType.withNameWithDefault(allParam.getDqConfig.getProcType)
    val dqApp: DQApp = procType match {
      case BatchProcessType => BatchDQApp(allParam)
      case StreamingProcessType => StreamingDQApp(allParam)
      case _ =>
        error(s"$procType is unsupported process type!")
        sys.exit(-4)
    }

    val job_name = allParam.getDqConfig.getName

    startup()

    // dq app init
    dqApp.init match {
      case Success(_) =>
        info("process init success")
      case Failure(ex) =>
        error(s"process init error: ${ex.getMessage}", ex)
        try {
          shutdown(job_name)
        } catch {
          case e: Exception =>
            error(e.getMessage, e)
            sys.exit(-5)
        }
        sys.exit(-5)
    }

    // dq app run
    val success = dqApp.run match {
      case Success(result) =>
        info("process run result: " + (if (result) "success" else "failed"))
        result

      case Failure(ex) =>
        error(s"process run error: ${ex.getMessage}", ex)

        if (dqApp.retryable) {
          throw ex
        } else {
          try {
            shutdown(job_name)
          } catch {
            case e: Exception =>
              error(e.getMessage, e)
              sys.exit(-5)
          }
          sys.exit(-5)
        }
    }

    // dq app end
    dqApp.close match {
      case Success(_) =>
        info("process end success")
      case Failure(ex) =>
        error(s"process end error: ${ex.getMessage}", ex)
        try {
          shutdown(job_name)
        } catch {
          case e: Exception =>
            error(e.getMessage, e)
            sys.exit(-5)
        }
        sys.exit(-5)
    }

    try {
      shutdown(job_name)
    } catch {
      case e: Exception =>
        error(e.getMessage, e)
        sys.exit(-5)
    }

    if (!success) {
      sys.exit(-5)
    } else {
      sys.exit(0)
    }
  }

  def readParamFile[T <: Param](file: String)(implicit m: ClassTag[T]): Try[T] = {
    val paramReader = ParamReaderFactory.getParamReader(file)
    paramReader.readConfig[T]
  }

  private def startup(): Unit = {}

  private def shutdown(job_name: String): Unit = {
    val directoryName = s"/data01/tmp/${job_name}"
    val outputDir = new File(directoryName)
    if (outputDir.exists()) {
      val directoryStream = Files.walk(Paths.get(directoryName)).iterator().asScala.toStream
      griffinLogger.info(s"Processing paths in ${directoryName}:")
      directoryStream
        .filter(Files.isDirectory(_))
        .filter(_.toString.endsWith(".csv"))
        .foreach(mergeFiles(_))

      FileUtils.deleteDirectory(outputDir);
      griffinLogger.info("Done!")
    }
  }

  private def mergeFiles(dir: Path): Unit = {
    val outputFilePath =
      dir.toString.replaceAll("/data01/tmp/griffin-matcher[^/]*", "").replaceAll("//", "/")
    val directoryStream = Files.list(dir).iterator().asScala.toStream
    val csvFiles = directoryStream
      .filter(Files.isRegularFile(_))
      .filter(_.toString.endsWith(".csv"))
      .map(_.toFile)
      .toList
    val outputFile = new File(outputFilePath)
    val parentDir = outputFile.getParentFile().getAbsolutePath()
    val directory = new File(parentDir)
    directory.mkdirs()
    if (outputFile.exists()) {
      griffinLogger.info(s"File ${outputFilePath} already exists.")
      if (outputFile.delete()) {
        griffinLogger.info(s"File ${outputFilePath} was deleted.")
      } else {
        throw new Exception(s"Couldn't delete file ${outputFilePath}")
      }
    }
    if (outputFile.createNewFile()) {
      val writer = new BufferedWriter(new FileWriter(outputFile))
      csvFiles.zipWithIndex.foreach {
        case (f, idx) =>
          griffinLogger.info(s"Processing ${f.toString}")
          var reader: BufferedReader = null
          try {
            reader = new BufferedReader(new FileReader(f))
            var line = reader.readLine()
            var isHeader = true
            while (line != null) {
              if ((isHeader && idx == 0) || !isHeader) {
                writer.write(line)
                writer.newLine()
              }
              isHeader = false
              line = reader.readLine()
            }
          } finally {
            if (reader != null) {
              reader.close()
            }
          }
      }
      writer.close()
    }
  }
}

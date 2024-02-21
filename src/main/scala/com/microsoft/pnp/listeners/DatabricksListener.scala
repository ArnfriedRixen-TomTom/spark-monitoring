package com.microsoft.pnp.listeners

import org.apache.logging.log4j.LogManager
import org.apache.spark.scheduler._

/**
 * Implement here your the ListenerEvents you want to capture by overriding methods available in the SparkListener interface.
 * We have provided a set of ListenerEvents that are useful in most monitoring cases :
 *
 * SparkListenerJobStart
 * SparkListenerJobEnd
 * SparkListenerApplicationStart
 * SparkListenerApplicationEnd
 *
 * If you want to add other ListenerEvents, be careful as some of them are extremely verbose and cannot be converted as JSON effectively by using ListenerUtils.parse().
 * In this case, you must construct the JSON by your own.
 * As an example, SparkListenerJobEnd cannot be deserialized properly and must be build manually to ensure we don't lose any field
 *
 *
 */
class DatabricksListener extends SparkListener {

  private val LOGGER = LogManager.getLogger();

  // Retrieve the filterRegex from the environment variable
  private var filterRegex = sys.env.getOrElse("LA_SPARKLISTENEREVENT_REGEX", "")


  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    processEvent(jobStart)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobResult = jobEnd.jobResult match {
      case JobSucceeded => "Success"
      case _ => "Failure"
    }
    val formattedClassName = SparkListenerJobEnd.getClass.getName.replace("$", "")

    val event = Map("Event" -> formattedClassName, "jobId" -> jobEnd.jobId.toString, "time" -> jobEnd.time.toString, "jobResult" -> jobResult)
    
    // Check if the regex pattern is empty or if the event name matches the regex
    if (filterRegex == "" || formattedClassName.matches(filterRegex)) {
      try {
        val json = ListenerUtils.parse(event)
        ListenerUtils.sendEvent(json)
      } catch {
        case e: Exception =>
          LOGGER.error("Could not parse event " + formattedClassName)
          LOGGER.error(e.getMessage)
      } 
    } else {
      // silently ignore the event
    }
  }
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    processEvent(applicationStart)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    processEvent(applicationEnd)
  }


  private def processEvent(listenerEvent: SparkListenerEvent): Unit = {
    val eventSimpleName = listenerEvent.getClass.getSimpleName
    // Check if the regex pattern is empty or if the event name matches the regex
    if (filterRegex=="" || eventSimpleName.matches(filterRegex)) {
      try {
        ListenerUtils.sendListenerEventToLA(listenerEvent)
      } catch {
        case e: Exception =>
          LOGGER.error("Could not parse event " + listenerEvent.getClass.getName)
          LOGGER.error(e.getMessage)
      }
    } else {
      // silently ignore the event
    }
  }
}

package aprsbigdata.parser

import java.io._
import scala.io._
import aprsbigdata.maps.kml._
import scala.collection.mutable.ListBuffer
import java.util.zip.GZIPInputStream

/**
 * This processes the output from the MapReduce job.
 * NOTE that for now this is iterative, as the size of the file is relatively small in comparison,
 * but this should clearly be converted into mapreduce form in the future.
 */

object ParseAPRSMapredOut {
  def parseLine(line: String) = {
    val tuples = line.split("\\s+");
    val iGateCallsign = tuples(0);
    val callerCallsign = tuples(1);
    val timestamp = tuples(2).toLong;
    val lat = tuples(3).toDouble;
    val long = tuples(4).toDouble;
    new QARMarker(iGateCallsign, callerCallsign, long, lat, timestamp = timestamp);
  }

  /**
   * Given a source reader, and a filter function that takes (iGateCallsign, PlaceMaker) => Boolean
   * returns the set of PlaceMarkers associated
   */
  def parseFromReader(lines:Iterator[String], filterFn: ((QARMarker) => Boolean)) = {
    val ret = new ListBuffer[QARMarker]();
    for (line <- lines) {
      val qARMarker = parseLine(line);
      if (filterFn(qARMarker)) {
    	  ret += qARMarker;
      }
    }
    ret.toList;
  }

  def parseFromFile(srcFile:File, filterFn:(QARMarker) => Boolean) = {
    val inputStream = if (srcFile.getName.endsWith(".gz")) {
      new GZIPInputStream(new FileInputStream(srcFile));
    } else {
      new FileInputStream(srcFile);      
    }
    val lines = Source.fromInputStream(inputStream).getLines;
    parseFromReader(lines, filterFn);
  } 
  
  //
  //Convenience routines for generating filtering functions, based on callsign as well as in grid
  //
  def makeInIGateCallsignsFn(callsigns: String*) = {
    val callsignSet = callsigns.toSet;
    (x:QARMarker) => callsignSet.contains(x.iGateCallsign);
  }

  def makeInGridFn(longLow: Double, longHigh: Double, latLow: Double, latHigh: Double) = {
    (x:QARMarker) =>
      {
        within(x.longitude, longLow, longHigh) && within(x.latitude, latLow, latHigh);
      }
  }

  def makeInTimestampFn(timestampLow: Long, timestampHigh: Long) = {
    (x:QARMarker) =>
      {
        timestampLow <= x.timestamp && x.timestamp <= timestampHigh;
      }
  }

  def within(x: Double, low: Double, high: Double): Boolean = { low <= x && x <= high };
}
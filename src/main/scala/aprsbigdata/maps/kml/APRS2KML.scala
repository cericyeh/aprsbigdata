package aprsbigdata.maps.kml

import java.io._
import scala.io._;
import scala.collection.mutable.ListBuffer

object APRS2KML {

  def main(args: Array[String]) = {
    if (args.size < 4) {
      println("USAGE: srcFile station_callsign station_longitude station_latitude [output_file]");
      println("Coordinates are in degree decimal form.");
      System.exit(-1);
    } else {
      val srcFile = new File(args(0));
      val callsign = args(1);
      val longitude = args(2).toDouble;
      val latitude = args(3).toDouble;
      if (args.size >= 5) {
        val outFile = new File(args(4));
        outFile.getParentFile.mkdirs;
        val out = new FileWriter(outFile);
        out.write(apply(srcFile, callsign, longitude, latitude));
        out.close();
        println("Wrote out to " + outFile.getAbsolutePath());
      } else {
        println(apply(srcFile, callsign, longitude, latitude));
      }

    }
  }

  def pointsToKML(datums: Iterator[String], tgtIGate: DigipeaterInfo,
    labelAltitude: Double = 1000.0,
    labelBearing: Double = 90.0): String = {
    val out = new StringWriter();
    def emit(str: String) = out.write(str + "\n");
    emit("""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">""");
    emit("<Document>");
    emit("<name>Pings for iGate " + tgtIGate + "</name>");
    emit(KMLUtils.defaultStyles);
    emit("<Placemark>");
    emit("<name>" + tgtIGate.callsign + "</name>");
    emit("<styleUrl>#stationOfInterest</styleUrl>");
    emit("<Point>");
    emit("<coordinates>");
    emit(List(tgtIGate.longitude, tgtIGate.latitude, tgtIGate.altitude).map(x => x.toString).mkString(","));
    emit("</coordinates>");
    emit("</Point>");
    emit("<LookAt>");
    emit("<longitude>");
    emit(tgtIGate.longitude.toString);
    emit("</longitude>");
    emit("<latitude>");
    emit(tgtIGate.latitude.toString);
    emit("</latitude>");
    emit("<altitude>");
    emit(tgtIGate.altitude.toString);
    emit("</altitude>");
    emit("</LookAt>");
    emit("</Placemark>\n");

    // For test, list the 50k-100km ring around the iGate
    for (
      radius <- List(5, 10, 15, 20, 25, 50, 75, 100);
      labelBearing <- List(0, 90, 180, 270)
    ) {
      emit("<Placemark>");
      emit("<name>" + radius + "km</name>")
      emit("<styleUrl>#APRSGeo</styleUrl>");
      emit(KMLUtils.kmlCircle(radius, tgtIGate.longitude, tgtIGate.latitude, altitude = 100.0, numPoints = 1000));
      emit("</Placemark>");
      emit("<Placemark>");
      emit("<name>" + radius + "km</name>");
      emit("<styleUrl>#rangeMarker</styleUrl>");
      val (refLong, refLat) = KMLUtils.getDestPt(radius, tgtIGate.longitude, tgtIGate.latitude, labelBearing);
      emit(KMLUtils.getPointKML(refLong, refLat, labelAltitude));
      emit("</Placemark>");
    }

    for (datum <- datums) {
      val tuples = datum.split("\\t");
      val iGate = tuples(0);
      if (tgtIGate.callsign == iGate) {
        val callsign = tuples(1);
        val timestamp = tuples(2).toLong;
        val lat = tuples(3).toDouble;
        val long = tuples(4).toDouble;
        val packet = tuples(5);
        emit("<Placemark>\n");
        //        emit("<name>" + callsign + "</name>\n");
        emit("<description>" + packet.split(":", 2).head.replaceAll(">", "&gt;") + "</description>\n");
        emit("<styleUrl>packetMarker</styleUrl>");
        emit("<Point>\n");
        emit("<coordinates>" + long + "," + lat + ",0</coordinates>\n"); // Note: we don't accommodate altitude for now
        emit("</Point>\n");
        emit("</Placemark>\n");
      }
    }
    emit("</Document>\n");
    emit("</kml>\n");
    out.toString();
  }

  def apply(datumsFile: File, callsign: String, long: Double, lat: Double, altitude: Double = 0) = {
    val lines = Source.fromFile(datumsFile).getLines;
    val sourceInfo = new DigipeaterInfo(callsign, long, lat, altitude);
    pointsToKML(lines, sourceInfo);
  }
}


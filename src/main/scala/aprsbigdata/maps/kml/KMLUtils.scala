package aprsbigdata.maps.kml

import java.io._
import scala.io._;
import scala.collection.mutable.ListBuffer

object KMLUtils {

  def kmlDocHeader(name: String = "") = {
    val out = new StringWriter();
    def emit(str: String) = out.write(str + "\n");
    emit("""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">""");
    emit("<Document>");
    emit("<name>" + name + "</name>");
    emit(defaultStyles);
    out.toString();
  }

  val kmlDocFooter = "</Document></kml>\n";

  val defaultStyles = """
    <Style id="APRSGeo">
      <LineStyle>
		  <color>ffa0a000</color>
		  <width>3</width>
      </LineStyle>
    </Style>
    <Style id="rangeMarker">
        <IconStyle>
		  <Icon>
		    <href>http://earth.google.com/images/kml-icons/track-directional/track-8.png</href>
          </Icon>
		  <scale>0.60</scale>
        </IconStyle>
    </Style>
     <Style id="packetMarker">
        <IconStyle>
		  <Icon>
		    <href>http://maps.google.com/mapfiles/kml/paddle/grn-diamond.png</href>
		  </Icon>
		  <scale>0.85</scale>
        </IconStyle>
    </Style> 
    <Style id="stationOfInterest">
       <IconStyle>
		  <Icon>
		    <href>http://maps.google.com/mapfiles/kml/paddle/red-stars.png</href>
		  </Icon>
          <scale>1.20</scale>
       </IconStyle>
    </Style>
    <Style id="highPing">
    <LineStyle>
      <width>1.5</width>
    <color>a078FF00</color>
    </LineStyle>
    <PolyStyle>
      <color>a078FF00</color>
    </PolyStyle>
  </Style>
    <Style id="medPing">
    <LineStyle>
      <width>1.5</width>
    <color>a078FFF0</color>
    </LineStyle>
    <PolyStyle>
      <color>a078FFF0</color>
    </PolyStyle>
  </Style>
    <Style id="lowPing">
    <LineStyle>
      <width>1.5</width>
    <color>a01478FF</color>
    </LineStyle>
    <PolyStyle>
      <color>a01478FF</color>
    </PolyStyle>
    </Style>
       <Style id="reallyLowPing">
    <LineStyle>
      <width>1.5</width>
          <color>a01400FF</color>
    </LineStyle>
    <PolyStyle>
      <color>a01400FF</color>
    </PolyStyle>
  </Style>

    """;

  def kmlCircle(radius: Double, longOrigin: Double, latOrigin: Double, altitude: Double = 0,
      numPoints: Int = 1000, color:String="ffa0a000", lineWidth:Int=3) = {
    val out = new StringWriter();
    def emit(str: String) = out.write(str + "\n");
    emit("<LineString>");
    emit("<LineStyle>");
    emit("<color>"+color+"</color>");
    emit("<width>"+lineWidth+"</width>");
    emit("</LineStyle>");
    emit("<visibility>1</visibility>");
    emit("<extrude>1</extrude>");
    emit("<altitudeMode>1</altitudeMode>");
    emit("<coordinates>");
    for ((long, lat) <- circlePoints(longOrigin, latOrigin, radius, numPoints)) {
      emit(long + "," + lat + "," + altitude);
    }
    emit("</coordinates>");
    emit("</LineString>");
    out.toString();
  }

  /**
   * Draws a KML fragment describing a ring originating from the origin longitude and altitude.  The ring is
   * described by the radius of the outer and inner rings, given in km.
   */
  def kmlRingPolygon(radiusOuter: Double, radiusInner: Double, longOrigin: Double, latOrigin: Double,
      color:String="1a0000ff",
    numPoints: Int = 1000, ringAltitude:Double = 20000.0) = {
    val out = new StringWriter();
    def emit(str: String) = out.write(str + "\n");
    emit("<Polygon>");
      emit("<extrude>1</extrude>");
     emit("<altitudeMode>relativeToGround</altitudeMode>");
   // emit("<altitudeMode>absolute</altitudeMode>");
    emit("<outerBoundaryIs>");
    emit("<LinearRing>");
    emit("<coordinates>");
    for ((long, lat) <- circlePoints(longOrigin, latOrigin, radiusOuter, numPoints)) {
      emit(long + "," + lat + "," + ringAltitude);
    }
    emit("</coordinates>");
    emit("</LinearRing>");
    emit("</outerBoundaryIs>");
    emit("<innerBoundaryIs>");
    emit("<LinearRing>");
    emit("<coordinates>");
    for ((long, lat) <- circlePoints(longOrigin, latOrigin, radiusInner, numPoints)) {
      emit(long + "," + lat + "," + ringAltitude);
    }
    emit("</coordinates>");
    emit("</LinearRing>");
    emit("</innerBoundaryIs>");
    emit("</Polygon>");
    emit("<PolyStyle>");
    emit("<Color>" + color + "</Color>");
    emit("<outline>1</outline>");
    emit("</PolyStyle>");
    out.toString();
  }

  def getPointKML(long: Double, lat: Double, altitude: Double) = {
    "<Point><coordinates>" + List(long, lat, altitude).mkString(",") + "</coordinates><altitudeMode>relativeToGround</altitudeMode><extrude>1</extrude></Point>"
  }
  
  def getLookAtKML(long: Double, lat: Double, altitude: Double) = {
    "<LookAt><longitude>"+long+"</longitude><latitude>"+lat+"</latitude><altitude>"+altitude+"</altitude></LookAt>";
  }

  val earthR = 6371.0; // Earth radius, km

  /**
   *  Inputs are in degrees, units returned are in km
   */
  def getDist(in_long1: Double, in_lat1: Double, in_long2: Double, in_lat2: Double) = {
    var dLat = Math.toRadians(in_lat2 - in_lat1);
    var dLon = Math.toRadians(in_long2 - in_long1);
    var lat1 = Math.toRadians(in_lat1)
    var lat2 = Math.toRadians(in_lat2);
    var a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.sin(dLon / 2) *
      Math.sin(dLon / 2) * Math.cos(lat1) * Math.cos(lat2);
    var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    c * earthR;
  }

  /**
   * Given a distance, origin longitude and latitude, and bearing (radians), gives the
   * target destination point as a (longitude,latitude) tuple
   * NOTE: inputs and outputs should be in degrees
   */
  def getDestPt(dist: Double, in_longOrigin: Double, in_latOrigin: Double, in_bearing: Double) = {
    val longOrigin = Math.toRadians(in_longOrigin);
    val latOrigin = Math.toRadians(in_latOrigin);
    val bearing = Math.toRadians(in_bearing);
    val angDist = dist / earthR;
    var lat2 = Math.asin(Math.sin(latOrigin) * Math.cos(angDist) +
      Math.cos(bearing) * Math.cos(latOrigin) * Math.sin(angDist));
    var lon2 = longOrigin + Math.atan2(Math.sin(bearing) * Math.sin(angDist) * Math.cos(latOrigin),
      Math.cos(angDist) - Math.sin(latOrigin) * Math.sin(lat2));
    (Math.toDegrees(lon2), Math.toDegrees(lat2));
  }

  def circlePoints(longitude: Double, latitude: Double, radius: Double, numPoints: Int = 1000): Iterable[(Double, Double)] = {
    val coords = new ListBuffer[(Double, Double)](); // tuples (longitude, latitude)
    val delta = 360.0 / numPoints;
    var degree = 0.0;
    for (i <- (0 until numPoints)) {
      val radsPair = getDestPt(radius, longitude, latitude, degree);
      coords += radsPair;
      degree += delta;
    }
    coords;
  }

}
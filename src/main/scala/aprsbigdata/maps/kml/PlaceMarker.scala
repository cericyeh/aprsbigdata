package aprsbigdata.maps.kml

class PlaceMarker(val label: String,
  val longitude: Double,
  val latitude: Double,
  val altitude: Double = 0,
  val bearing: Double = 0.0,
  val timestamp: Long = 0) {
  override def toString() = label + " " + List(longitude, latitude, altitude, bearing).mkString(",");
}

/**
 * Represents one of the qAR one-hops we identified, with the iGate callsign and the caller information.
 */
class QARMarker(val iGateCallsign:String, callerCallsign: String, longitude: Double, 
    latitude: Double, altitude: Double = 0.0, bearing: Double = 0.0, timestamp:Long=0, var distance:Double=0.0)
  extends PlaceMarker(callerCallsign, longitude, latitude, 
      altitude=altitude, bearing=bearing, timestamp=timestamp) {
  override def toString() = {
    label+"=>"+iGateCallsign+" "+List(longitude, latitude, altitude, bearing).mkString(",")+" dist="+distance;
  }
}

class DigipeaterInfo(val callsign: String, val longitude: Double, val latitude: Double, val altitude: Double = 0) {
  override def toString() = callsign + " " + List(longitude, latitude, altitude).mkString(",");
}

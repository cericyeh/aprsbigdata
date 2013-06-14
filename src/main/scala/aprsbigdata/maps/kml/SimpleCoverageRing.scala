package aprsbigdata.maps.kml

import java.io._
import scala.collection.mutable.{ ListBuffer, HashMap }
import scala.collection.mutable.HashSet
import aprsbigdata.util.Freqs

class SimpleCoverageRing(val centerCallsign: String,
  var originLongitude: Double = 0.0, var originLatitude: Double = 0.0,
  var originAltitude: Double = 0.0) {
  val pings = new ListBuffer[QARMarker]();

  def size() = pings.size;

  def setOriginLatLong(lat: Double, long: Double) {
    originLongitude = long;
    originLatitude = lat;
    centerInferred = false;
    invalidateCaches();
  }

  var centerInferred: Boolean = false;
  /**
   * Hack: given a set of one hop points, presume the origin (iGate)
   * coordinates are the means of the lat/longs
   */
  def setCoordsFromMean() {
    var latSum = 0.0;
    var longSum = 0.0;
    pings.foreach(x => {
      longSum += x.longitude
      latSum += x.latitude;
    });
    centerInferred = true;
    originLongitude = longSum / pings.size;
    originLatitude = latSum / pings.size
    invalidateCaches();
  }

  def +=(pm: QARMarker) {
    pings += pm;
    invalidateCaches();
  }

  private var lastOrdering: Option[Iterable[QARMarker]] = None;

  def reverseRangeSorted() = {
    lastOrdering match {
      case Some(x) => x;
      case None => {
        pings.foreach(x => {
          val dist = KMLUtils.getDist(originLongitude, originLatitude, x.longitude, x.latitude);
          x.distance = dist;
        });
        val sorted = pings.toList.sortWith(_.distance > _.distance);
        lastOrdering = Some(sorted);
        sorted
      }
    }
  }

  def invalidateCaches() = lastOrdering = None;

  def distinctByLocation(pingList: Iterable[QARMarker]) = {
    val ret = new ListBuffer[QARMarker]();
    val seen = new HashSet[(Double, Double)](); // (long, lat)
    for (ping <- pingList) {
      val coord = (ping.longitude, ping.latitude);
      if (!seen.contains(coord)) {
        ret += ping;
        seen += coord;
      }
    }
    ret;
  }

  def binnedByRange(binWidth: Integer = 5, forceDistinct: Boolean = true) = {
    val freq = new Freqs[Integer]();
    val toBin = if (forceDistinct) distinctByLocation(pings) else pings;
    toBin.foreach(x => freq.inc((Math.round(x.distance / binWidth.toDouble) * binWidth).toInt));
    freq.toListByKey();
  }

  def getHeardRanges() = {
    val ret = new ListBuffer[HeardPoint]();
    var num = 0;
    val pingsToCheck = distinctByLocation(reverseRangeSorted());
    for (ping <- pingsToCheck) {
      num += 1;
      num match {
        case 100 => ret += new HeardPoint(ping.distance, num);
        case 50 => ret += new HeardPoint(ping.distance, num);
        case 10 => ret += new HeardPoint(ping.distance, num);
        case 1 => ret += new HeardPoint(ping.distance, num);
        case _ =>  ;
      }
    }
    ret += new HeardPoint(0, num);
    ret.toList;
  }

  class HeardPoint(val range: Double, val num: Int) {
    override def toString() = { range+"km #="+num;}
  }

  // Be careful about calling this repeatedly
  def maxRange() = reverseRangeSorted().head.distance;

  def toKML(binWidth: Integer = 50) = {
    import SimpleCoverageRing.df;
    val outerKM = maxRange();
    val out = new StringWriter();
    val sortedByRange = reverseRangeSorted();
    def emit(str: String) = out.write(str + "\n");
    emit("<Folder>");
    emit("<name>" + centerCallsign + (if (centerInferred) " (inferred loc)" else "") + "</name>");
    emit("<Placemark>");
    emit("<name>" + centerCallsign + "</name>");
    emit("<description> Number packets = " + size() + ", long,lat=(" + originLongitude + "," + originLatitude + ")" + (if (centerInferred) " INFERRED FROM MEAN" else "") + "</description>");
    emit("<styleUrl>#stationOfInterest</styleUrl>");
    emit(KMLUtils.getPointKML(originLongitude, originLatitude, originAltitude));
    emit(KMLUtils.getLookAtKML(originLongitude, originLatitude, originAltitude));
    emit("</Placemark>");
    emit("<Placemark>");
    emit("<name> Furthest distance heard one-hop, " + df.format(outerKM) + "km</name>");
    emit("<styleUrl>#APRSGeo</styleUrl>");
    emit(KMLUtils.kmlCircle(outerKM, originLongitude, originLatitude));
    emit("</Placemark>");
    emit("<Folder>");
    emit("<name>OTA Packets</name>");
    for (ping <- distinctByLocation(pings)) {
      emit("<Placemark>");
      //        emit("<name>" + callsign + "</name>\n");
      emit("<description>" + ping.label + "&gt;" + centerCallsign + ", dist=" + df.format(ping.distance) + "</description>\n");
      emit("<styleUrl>packetMarker</styleUrl>");
      emit("<Point>");
      emit("<coordinates>" + ping.longitude + "," + ping.latitude + "," + ping.altitude + "</coordinates>\n"); // Note: we don't accommodate altitude for now
      emit("</Point>");
      emit("</Placemark>");
    }
    emit("</Folder>");

    // emit ranges of heard
    emit("<Folder>");
    emit("<name>Pessimistic Histogram for "+centerCallsign+"</name>");
    for (pair <- getHeardRanges.sliding(2)) {
      var pessimisticNum = 0.0;
      if (pair.size == 1) {
        ;
      } else {
        val outer = pair(0);
        val inner = pair(1);
        pessimisticNum = outer.num;
        val styleUrl:String = "#"+(if (pessimisticNum >= 100) {
          "highPing"
        } else if (pessimisticNum >= 50){
          "medPing"
        } else if (pessimisticNum>= 10) {
          "lowPing"
        } else {
          "reallyLowPing";
        });
        emit("<Placemark>");
        emit("<name> Pessimistic count="+pessimisticNum+" at "+df.format(outer.range)+"km</name>");
        emit("<styleUrl>" + styleUrl + "</styleUrl>");
        val ringKML = KMLUtils.kmlRingPolygon(outer.range.toDouble, inner.range.toDouble, originLongitude, originLatitude, ringAltitude = 20000.0, numPoints = 1000);
        emit(ringKML);
        emit("</Placemark>");
      }
    }
    emit("</Folder>");

  
    emit("</Folder>");
    out.toString();
  }
}

object SimpleCoverageRing {
  val df = new java.text.DecimalFormat("#.####");
  def sortByIGate(qARMarkers: Iterable[QARMarker], minPings: Option[Int] = None) = {
    val ret = new HashMap[String, SimpleCoverageRing]();
    for (qARMarker <- qARMarkers) {
      val ring = ret.getOrElseUpdate(qARMarker.iGateCallsign, new SimpleCoverageRing(qARMarker.iGateCallsign));
      ring += qARMarker;
    }
    for (ring <- ret.values) {
      ring.setCoordsFromMean();
    }
    ret;
  }
}
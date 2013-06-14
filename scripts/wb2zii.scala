import java.io._;
import scala.io._;
import scala.collection.mutable.{ListBuffer,HashMap,HashSet}
import net.ab0oo.aprs.parser._;
import aprsbigdata.parser._;
import aprsbigdata.maps.kml._;
import aprsbigdata.util._;

val callsigns = Array("WB2ZII", "WB2ZII-15", "WB2ZII-14", "WB2ZII-13");
//val tgtFile = new File("output/all-2012.log.gz");
val tgtFile = new File("output/wb2zii.log");

val filterFn = ParseAPRSMapredOut.makeInIGateCallsignsFn("WB2ZII", "WB2ZII-15", "WB2ZII-14", "WB2ZII-13");

val startTime = new java.util.Date();
val inBox = ParseAPRSMapredOut.parseFromFile(tgtFile, filterFn)
val endTime = new java.util.Date();

val byIGate = SimpleCoverageRing.sortByIGate(inBox);
val filtered = new ListBuffer[SimpleCoverageRing]();
for ((iGateCallsign, ring) <- byIGate) {
  if (ring.size >= 1) {
	  filtered += ring;
  }
}

// NOTE: according to aprs.fi, these appear to be mobile
// stations.  However, for the purposes of this demo we treat these
// as static, and set their positions.
val knownPositions = Map(
    "WB2ZII" -> (-73.4825, 41.0467),
    "WB2ZII-13" -> (-73.3350, 41.1950),
    "WB2ZII-14" -> (-73.5350, 41.1850),
    "WB2ZII-15" -> (-73.4850, 41.1650)
    );

val out = new FileWriter(new File("output/aprs/w2bzii.kml"));
out.write(KMLUtils.kmlDocHeader("W2BZII Stations Check"));
for (ring <- filtered) {
  if (knownPositions.contains(ring.centerCallsign)) {
    val longLat = knownPositions(ring.centerCallsign);
    ring.setOriginLatLong(longLat._2, longLat._1);
  }
  out.write(ring.toKML());
}
out.write(KMLUtils.kmlDocFooter);
out.close();
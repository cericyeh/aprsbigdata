
val tgtFile = new File("output/all-2012.log.gz");

// Set the filter function to identify a box roughly 
// grabbing the SF Bay Area
val filterFn = ParseAPRSMapredOut.makeInGridFn(-122.3029, -122.02, 37.20, 38.05);

val startTime = new java.util.Date();
val inBox = ParseAPRSMapredOut.parseFromFile(tgtFile, filterFn)
val endTime = new java.util.Date();

val byIGate = SimpleCoverageRing.sortByIGate(inBox);
val filtered = new ListBuffer[SimpleCoverageRing]();
for ((iGateCallsign, ring) <- byIGate) {
  if (ring.size >= 1000) {
	  filtered += ring;
  }
}

// NOTE: according to aprs.fi, these appear to be mobile
// stations.  However, for the purposes of this demo we treat these
// as static, and set their positions.
val knownPositions = Map(
    "KC6SSM-5" -> (-122.1692,37.5416),
    "W6YX-5"->(-122.0994, 37.2421) 
    );

val out = new FileWriter(new File("output/aprs/sfbayarea.kml"));
out.write(KMLUtils.kmlDocHeader("Test"));
for (ring <- filtered) {
  if (knownPositions.contains(ring.centerCallsign)) {
    val longLat = knownPositions(ring.centerCallsign);
    ring.setOriginLatLong(longLat._2, longLat._1);
  }
  out.write(ring.toKML());
}
out.write(KMLUtils.kmlDocFooter);
out.close();
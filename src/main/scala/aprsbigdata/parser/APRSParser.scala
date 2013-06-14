package aprsbigdata.parser

import java.io._;
import scala.io._
import net.ab0oo.aprs.parser._;

object APRSParser {
  /**
   * This is to account for fact that javAPRSLib doesn't seem to deal with prefixing timestamps
   */
  def parse(line: String) = {
    val splitByQ = splitByQConstruct(line).get;
    val qConstruct = splitByQ(2);
    val tuples = line.split("\\s+", 2);
    val timestamp = tuples(0).toLong;
    val packetString = tuples(1);
    val rawPacket = Parser.parse(packetString);
    
    new APRSDatum(timestamp, rawPacket, qConstruct);
  }
  
  //
  // Below are routines for manual parseout of the timestamped string
  //
  
  /**
   * Splits a line by the Q construct code, returning a seq with (string before, string after, qXX)
   */
  def splitByQConstruct(input: String): Option[Seq[String]] = {
    val qPat = ",(q[a-zA-Z][a-zA-Z]),".r
    qPat.findFirstMatchIn(input) match {
      case Some(matchObj) => {
        Some(List(input.slice(0, matchObj.start), input.slice(matchObj.end, input.size), matchObj.group(1)));
      }
      case None => None;
    }
  }

  /*def parseleft(input: String) = {
    val tuples = input.split(",");
    val (origStation, destStation) = tuples(0).split(">");
    val stations = tuples.slice(1, tuples.size);
    List(origStation, destStation) ++ stations;
  }*/

}

class APRSDatum(val timestamp: Long, val packet: APRSPacket, val qConstruct:String) {
  override def toString() = timestamp + " " + packet.toString();
}
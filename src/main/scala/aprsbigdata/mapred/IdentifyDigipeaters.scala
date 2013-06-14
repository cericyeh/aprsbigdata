package aprsbigdata.mapred

import org.apache.hadoop.mapreduce.{ Mapper, Partitioner, Reducer, Job }
import org.apache.hadoop.conf.{ Configuration, Configurable }
import org.apache.hadoop.io.{ LongWritable, Text }
import org.slf4j._
import aprsbigdata.parser._
import net.ab0oo.aprs.parser._
import scala.collection.JavaConversions._
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.{ MultipleOutputs, TextOutputFormat, FileOutputFormat }
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.util.Progressable

/**
 * This simply identifies iGate digipeaters of interest, and extracts all to an output file
 * named with the digipeater callsign.
 *
 * TODO: generate composite key allowing us to sort digipeater callsigns by timestamp
 */

class FindDigipeatersMapper extends Mapper[LongWritable, Text, Text, Text] {
  val logger = LoggerFactory.getLogger("aprsbigdata.mapred.FindDigipeaterMapper");
  var digipeaters: Set[String] = Set[String]();

  override def setup(context: Mapper[LongWritable, Text, Text, Text]#Context) {
    val conf = context.getConfiguration();
    val digiStr = conf.get(FindDigipeaters.TGT_DIGIPEATERS);
    if (digiStr != null) {
      digipeaters = digiStr.split(",").toSet;
    }
  }

  /**
   * This simply filters, by iGate, the APRS packets whose iGates are given in the digipeaters list.
   * We deal only with over the air broadcasts (qAR), generating a key based on the iGate callsign
   * and the timestamp.  We partition using the callsign, but retain the timestamp so values arrive
   * in increasing alphanumeric order at the reducers.
   *
   * MAP OUTPUT: timestamp callsign latitude longitude   original_packet_value
   * NOTE: this requires the digipeaters list returned by javAPRSlib size be 2 (qConstruct, received)
   */
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context) {
    val outKey = new Text();
    val outValue = new Text();
    try {
      val parsed = APRSParser.parse(value.toString());
      // We only care about over the air pings, meaning:
      // - it's a "qAR"
      // - the digipeaters list (as returned by javAPRSlib, size is 2 (q construct, received)
      if (parsed.packet.getDigipeaters().size == 2 && parsed.qConstruct == "qAR") {
        val timestamp = parsed.timestamp;
        val packet = parsed.packet;
        val iGate = packet.getIgate();
        if (digipeaters.contains(iGate) || digipeaters.size() == 0) {
          outKey.set((new CallsignAndTimestamp(iGate, timestamp)).toString());
          if (packet.getAprsInformation().isInstanceOf[PositionPacket]) {
            val posPacket = packet.getAprsInformation().asInstanceOf[PositionPacket];
            val posObj = posPacket.getPosition();
            val latitude = posObj.getLatitude();
            val longitude = posObj.getLongitude();
            val valueString = List(iGate, packet.getSourceCall(), timestamp.toString(), latitude.toString(), longitude.toString(), packet.toString()).mkString("\t");
            outValue.set(valueString);
            context.write(outKey, outValue);
          }
        }
      }
    } catch {
      case e: Exception => {
        logger.error("Error in map, key=" + key.toString() + ", value=" + value.toString(), e);
      }
    }
  }
}

/**
 * We partition based on the iGate callsign, so all packets for a given callsign are sent
 * to just one reducer, in timestamp order.
 */
class FindDigipeatersPartitioner extends Partitioner[Text, Text] {
  def getPartition(key: Text, value: Text, numPartitions: Int): Int = {
    val keyObj = CallsignAndTimestamp(key.toString());
    val callsign = keyObj.callsign;
    val partitionNum = callsign.toString().hashCode() % numPartitions;
    partitionNum;
  }
}

/**
 * This pulls in the packets and writes them out.  We batch group these and
 * use the parallelization facility to use up any available cores to process
 * in parallel.
 */
class FindDigipeatersReducer extends Reducer[Text, Text, Text, NullWritable] {
  val logger = LoggerFactory.getLogger("aprsbigdata.mapred.FindDigipeaterReducer");
  var mos: Option[MultipleOutputs[Text, NullWritable]] = None;

  override def setup(context: Reducer[Text, Text, Text, NullWritable]#Context) {
    val digiStr = context.getConfiguration.get(FindDigipeaters.TGT_DIGIPEATERS);
    if (digiStr != null) {
      mos = Some(new MultipleOutputs[Text, NullWritable](context));
    }
  }

  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, NullWritable]#Context) {
    val textOut = new Text();
    try {
      for (value <- values) {
        try {
          val keyObj = CallsignAndTimestamp(key.toString());
          textOut.set(value);
          mos match {
            case Some(mosObj) => mosObj.write(keyObj.callsign, key, textOut);
            case None => ;
          }
          context.write(textOut, NullWritable.get)
        } catch {
          case e: Exception => {
            logger.error("Error writing out in reduce, key=" + key.toString() + ", value=" + value.toString(), e);
          }
        }
      }
    } catch {
      case e: Exception => {
        logger.error("Error in reduce, key=" + key.toString(), e);
      }
    }
  }
}

class FindDigipeaters {
  val logger = LoggerFactory.getLogger("aprsbigdata.mapred.FindDigipeater");

  import FindDigipeaters._;
  def run(csvInputPaths: String, outputPath: String, csvTgtDigipeaters: String) = {
    val conf = new Configuration();
    val outputDir = new Path(outputPath);
    conf.set(TGT_DIGIPEATERS, csvTgtDigipeaters);

    val fs = FileSystem.get(conf);
    if (fs.exists(outputDir)) {
      fs.delete(outputDir, true);
    }

    val job = new Job(conf);
    job.setJobName("ident_digipeaters");

    job.setMapperClass(classOf[FindDigipeatersMapper]);
    job.setPartitionerClass(classOf[FindDigipeatersPartitioner]);
    job.setReducerClass(classOf[FindDigipeatersReducer]);

    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[Text]);

    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[NullWritable]);

    job.setInputFormatClass(classOf[TextInputFormat]);
    job.setOutputFormatClass(classOf[TextOutputFormat[Text,NullWritable]])
    FileInputFormat.setInputPaths(job, csvInputPaths);
    FileOutputFormat.setOutputPath(job, outputDir);

    if (csvTgtDigipeaters.size > 0) {
      for (digipeater <- csvTgtDigipeaters.trim.split(",")) {
        MultipleOutputs.addNamedOutput(job, digipeater, classOf[TextOutputFormat[Text, Text]],
          classOf[Text], classOf[Text]);
      }
    } 
    job.setJarByClass(classOf[FindDigipeaters]);
    logger.info("Starting job, tgt digipeaters=" + csvTgtDigipeaters);
    val exitCode = if (job.waitForCompletion(true)) 0 else 1;
    if (exitCode == 0) {
      //
    }
    exitCode;
  }
}

object FindDigipeaters {
  val TGT_DIGIPEATERS = "target_digipeaters";

  def main(args: Array[String]) {
    if (args.size < 2) {
      System.out.println("USAGE inputPathsCSV outputPath digipeatersCSV");
      System.out.println("inputPathsCSV = comma separated list of input paths to pull logs from");
      System.out.println("outputPath = path to directory to store output files")
      System.out.println("digipeatersCSV = comma separated list of digipeater callsigns, if not specified, all valid over the air packets parsed");
      System.exit(-1);
    }
    val digipeaters = if (args.size < 3) "" else args(2);
    val fd = new FindDigipeaters();
    fd.run(args(0), args(1), digipeaters);
  }
}

class CallsignAndTimestamp(rawCallsign: String, val timestamp: Long) {
  val callsign = {
    // get rid of trailing aliases
    rawCallsign.split("-", 2).head;
  }
  override def toString() = callsign + "," + timestamp.toString;
}

object CallsignAndTimestamp {
  def apply(keyString: String) = {
    val tuples = keyString.split(",", 2);
    new CallsignAndTimestamp(tuples(0), tuples(1).toLong);
  }
}
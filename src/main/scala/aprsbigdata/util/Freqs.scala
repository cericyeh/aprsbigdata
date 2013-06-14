package aprsbigdata.util

import scala.collection.mutable.HashMap
import java.io.StringWriter

class Freqs[U <: Comparable[U]]() {
  val df = new java.text.DecimalFormat("#.#####");
  val vmap = new HashMap[U, Double]();

  def inc(x: U) = vmap(x) = vmap.getOrElse(x, 0.0) + 1.0;
  def total() = vmap.map(_._2).reduceLeft(_ + _);

  def toListByKey() = {
    vmap.toList.sortWith((x, y) => x._1.compareTo(x._1) < 0);
  }

  def toListByValues() = {
    vmap.toList.sortWith(_._2 > _._2);
  }

  override def toString() = {
    var ret = new StringWriter();
    ret.write("Total = " + df.format(total()) + "\n");
    for ((k, v) <- vmap.toList.sortWith(_._2 > _._2)) {
      ret.write(k + "\t" + df.format(v) + "\n");
    }
    ret.toString();
  }
}
package starter
import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.util.Try

// packet format http://www.plixer.com/support/netflow_v5.html


// 24 bytes of header
case class NetflowV5Header (
    val version:Short,
    val count:Short,
    val sysUptime:Int,
    val unixSecs:Int,
    val unixnSecs:Int,
    val sequence:Int,
    val engineTag:Short,
    val samplingInterval:Short
)

case class NetflowV5Record (
  srcAddr: Int, // 0
  dstAddr:Int,  // 4
  nextHop:Int,  //8
  inputIfIndex:Short, //12
  outputIfIndex:Short, //14
  numPkts:Int, //16
  numOctets:Int, //20
  first:Int, //24
  last:Int, //28
  srcPort:Short, //30
  dstPort:Short, //32
  pad1:Byte, //34
  tcpFlag:Byte, //35
  prot:Byte, //36
  tos:Byte, // 37
  srcAs:Short, // 38
  dstAs:Short, //40
  srcMask:Byte, //42
  dstMask:Byte, //43
  pad2:Short // 44
)

object NetflowV5Record {

   def apply(byteBuffer: ByteBuffer, offset: Int) : NetflowV5Record =  {
      NetflowV5Record(byteBuffer.getInt(offset + 0),
                    byteBuffer.getInt(offset + 4),
                    byteBuffer.getInt(offset + 8),
                    byteBuffer.getShort(offset + 12),
                    byteBuffer.getShort(offset + 14),
                    byteBuffer.getInt(offset + 16),
                    byteBuffer.getInt(offset + 20),
                    byteBuffer.getInt(offset + 24),
                    byteBuffer.getInt(offset + 28),
                    byteBuffer.getShort(offset + 30),
                    byteBuffer.getShort(offset + 32),
                    byteBuffer.get(offset + 34),
                    byteBuffer.get(offset + 35),
                    byteBuffer.get(offset + 36),
                    byteBuffer.get(offset + 37),
                    byteBuffer.getShort(offset + 38),
                    byteBuffer.getShort(offset + 40),
                    byteBuffer.get(offset + 42),
                    byteBuffer.get(offset + 43),
                    byteBuffer.getShort(offset + 44)
       )
  }
}

object NetflowV5Parser {

  private val netflowHeaderSize = 24
  private val netflowRecordSize = 48

  def intToIp(i:Int) :String = {
    return ((i >> 24 ) & 0xFF) + "." +
         ((i >> 16 ) & 0xFF) + "." +
         ((i >>  8 ) & 0xFF) + "." +
         (i & 0xFF)
  }

  def apply(byteBuffer: ByteBuffer, pktLength: Int): ArrayBuffer[NetflowV5Record] = {

    val version = byteBuffer.getShort(0)

    var v5Records = ArrayBuffer[NetflowV5Record]()
    if (version == 5){
      // netflow V5 packet 
      // read netflow record count 
      var netflowRecordCount = byteBuffer.getShort(2)
      var offset = netflowHeaderSize;
      //println("No of netflow records netflowRecordCount" + netflowRecordCount)
      for (i <- 1 to netflowRecordCount) {
        v5Records += NetflowV5Record(byteBuffer, offset)  
      }
    } 
    v5Records
  }
}

package starter
import java.net.DatagramPacket 
import java.net.DatagramSocket
// a udpClient to receive and parse netflow V5 record
object udbClient { 

	def intToIp(i:Int) :String = {
        return ((i >> 24 ) & 0xFF) + "." +
               ((i >> 16 ) & 0xFF) + "." +
               ((i >>  8 ) & 0xFF) + "." +
               ( i        & 0xFF);
  }
	val bufsize = 65535 // max UDP buffer size
	val port = 9999

	def main(args : Array[String]) : Unit = { 

		println("UDP client started...")

		val sock = new DatagramSocket(port)
		val buf = new Array[Byte](bufsize)
		val packet = new DatagramPacket(buf, bufsize)

		while (true) {
  		sock.receive(packet)
		  println("received packet from: " + packet.getAddress() + "Of Length" +  packet.getLength())
			val byteArray = packet.getData()
			val size = byteArray.size
			val byteBuffer = ByteBuffer.wrap(byteArray);
			val version = byteBuffer.getShort(0)
			if (version == 5) {
					val pkt = NetflowV5Parser(byteBuffer , size)
			}
		}
	}
}

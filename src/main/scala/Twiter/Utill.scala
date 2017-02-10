package Twiter

import twitter4j.{StallWarning, Status, StatusDeletionNotice, StatusListener}
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
/**
  * Created by vrai on 1/5/2017.
  */
object Utill {

  val config = new twitter4j.conf.ConfigurationBuilder()
    .setOAuthConsumerKey("KvzydZLUbCYbU71NLJESPf0TI")
    .setOAuthConsumerSecret("GoM1gf9kjeySgvKHZOcb0so3sNz1khcUYt1pI1RrolZdStCtMP")
    .setOAuthAccessToken("1968579055-GaHe7mIbYtqs0LcsRbVzjBcAlfV15N5QfQPsCh5")
    .setOAuthAccessTokenSecret("S3qTuZQM7z0FjS3uUDAhQa9Iv5wmuXyoTh86MggexZYRu")
    .build



  def simpleStatusListener = new StatusListener() {
    def onStatus(status: Status)
    {
      implicit val formats = Serialization.formats(NoTypeHints)
      //println(status.getText)
      println("fetching respective data............................")
      val user: String =write(UserDetail(status.getUser.getName,status.getUser.getScreenName,
          status.getUser.getLocation,status.getUser.getDescription))
      println("onstatus finish....................................."+user)
      TwitterKafkaProducer.send(user)
    }
    def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
    def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
    def onException(ex: Exception) { ex.printStackTrace }
    def onScrubGeo(arg0: Long, arg1: Long) {}
    def onStallWarning(warning: StallWarning) {}
  }
}
case class UserDetail(userName:String, screenName:String, location:String, description:String)

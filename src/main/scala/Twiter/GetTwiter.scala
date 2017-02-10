package Twiter

import twitter4j.{FilterQuery, TwitterStreamFactory}

/**
  * Created by vrai on 1/5/2017.
  */
object GetTwiter {

  def main(args: Array[String]) {
    val twitterStream = new TwitterStreamFactory(Utill.config).getInstance
    twitterStream.addListener(Utill.simpleStatusListener)
    //twitterStream.sample
    //twitterStream.getFilterStream(new FilterQuery().track(Array("#demonitasion")))
    twitterStream.filter(new FilterQuery().track(Array("#OmPuri")))
    //twitterStream.sample
    /*Thread.sleep(10000)
    twitterStream.cleanUp
    twitterStream.shutdown*/
  }

}

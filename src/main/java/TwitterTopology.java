import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import bolt.HashtagLoggerBolt;
import bolt.HashtagReaderBolt;
import bolt.OutputResultBolt;
import spout.TweetsStreamSpout;

/**
 * Created by Qiu on 11/8/15.
 */


public class TwitterTopology {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweet-reader", new TweetsStreamSpout());
//        builder.setBolt("hashtag-reader", new HashtagReaderBolt());
        builder.setBolt("tweet-hashtag-logger", new HashtagLoggerBolt()).shuffleGrouping("tweet-reader");

        Config config = new Config();
        config.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("twitter-job", config, builder.createTopology());
    }

}

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolt.OutputResultBolt;
import bolt.TweetHashtagCounterBolt;
import bolt.TweetHashtagLoggerBolt;
import spout.TweetsStreamSpout;

/**
 * Created by Qiu on 11/8/15.
 */


public class TwitterTopology {

    public static final int INTERVAL_SEC = 10;
    public static final int w = 50;
    public static final double s = 0.1;
    public static final String OUTPUT_PATH = "tweetlog";
    public static int COUNT_WORKER_NUM;

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

//        if (args.length < 2) {
//            System.err.println("Usage: TwitterTopology <remote/local> <lossy-counting-worker-number> <output-dir>");
//            System.exit(1);
//        }
//
        boolean isLocal = !args[0].equals("remote");
        COUNT_WORKER_NUM = Integer.parseInt(args[1]);
//        String outputPath = args[3];


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweet-extracter", new TweetsStreamSpout());

        builder.setBolt("tweet-hashtag-logger", new TweetHashtagLoggerBolt(OUTPUT_PATH))
                .shuffleGrouping("tweet-extracter", "hashtag-logger");

        builder.setBolt("hashtag-reader", new TweetHashtagCounterBolt(s, w, INTERVAL_SEC))
                .fieldsGrouping("tweet-extracter", "hashtag-counter", new Fields("hashtag"));
        builder.setBolt("hashtag-outputter", new OutputResultBolt(INTERVAL_SEC, OUTPUT_PATH))
                .shuffleGrouping("hashtag-reader");

        Config config = new Config();
        config.setDebug(false);
        if (isLocal) {
            LocalCluster cluster = new LocalCluster();
            config.setNumWorkers(COUNT_WORKER_NUM);
            cluster.submitTopology("twitter-job", config, builder.createTopology());
        } else {
            config.setNumWorkers(COUNT_WORKER_NUM);
            StormSubmitter.submitTopology("twitter-job", config, builder.createTopology());
        }
    }

}

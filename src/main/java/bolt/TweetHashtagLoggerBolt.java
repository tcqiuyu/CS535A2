package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Tuple;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.Map;

/**
 * Created by Qiu on 11/12/15.
 */
public class TweetHashtagLoggerBolt extends BaseRichBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        Status tweet = (Status) input.getValue(0);

        if (tweet.getHashtagEntities().length > 0) {
            System.out.print("<"+tweet.getCreatedAt()+">");
//            for (HashtagEntity hashtag : tweet.getHashtagEntities()) {
//                System.out.print("<"+hashtag.getText()+">");
//            }
            System.out.print(":" + tweet.getText());
            System.out.println();
            System.out.println("---------------------------------------------------------------------------------------");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}

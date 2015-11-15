package bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.Twitter;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by Qiu on 11/11/15.
 */
public class OutputResultBolt extends BaseBasicBolt{

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}

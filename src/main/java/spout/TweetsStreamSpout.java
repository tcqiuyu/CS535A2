package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Qiu on 11/10/15.
 */
public class TweetsStreamSpout extends BaseRichSpout {
    private static final int QUEUE_LENGTH = 1000;
    SpoutOutputCollector collector;
    LinkedBlockingQueue<Status> queue = null;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet-raw"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {}

            @Override
            public void onStallWarning(StallWarning warning) {}

            @Override
            public void onException(Exception ex) {}
        };
        queue = new LinkedBlockingQueue<>(QUEUE_LENGTH);
        this.collector = collector;
        TwitterStream twitterStream = new TwitterStreamFactory(
                new ConfigurationBuilder().setJSONStoreEnabled(true).build()).getInstance();
        twitterStream.addListener(listener);
        twitterStream.sample();
    }

    @Override
    public void nextTuple() {
        Status status = queue.poll();
        if (null == status) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(status));
        }
    }
}

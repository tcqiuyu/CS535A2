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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Qiu on 11/10/15.
 */
public class TweetsStreamSpout extends BaseRichSpout {
    private static final int QUEUE_LENGTH = 1000;
    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue = null;
    private ConcurrentHashMap<UUID, Values> pending;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("hashtag-counter", new Fields("hashtag"));
        declarer.declareStream("hashtag-logger", new Fields("tweet"));
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
        pending = new ConcurrentHashMap<>();
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

            HashtagEntity[] hashtags = status.getHashtagEntities();
            if (hashtags.length > 0) {
                for (HashtagEntity hashtag : hashtags) {
                    String hashtagText = hashtag.getText().toLowerCase();
                    UUID msgId = UUID.randomUUID();
                    pending.put(msgId, new Values("count", new Values(hashtagText)));
                    collector.emit("hashtag-counter", new Values(hashtagText), msgId);
                }
                UUID msgId = UUID.randomUUID();
                pending.put(msgId, new Values("log", new Values(status)));
                collector.emit("hashtag-logger", new Values(status));
            }

        }
    }

    @Override
    public void ack(Object msgId) {
        this.pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        Values failedItem = pending.get(msgId);
        String destBolt = (String) failedItem.get(0);
        if (destBolt.equals("count")) {
            this.collector.emit("hashtag-counter", (Values) failedItem.get(1), msgId);
        } else if (destBolt.equals("log")) {
            this.collector.emit("hashtag-logger", (Values) failedItem.get(1), msgId);
        }
    }
}

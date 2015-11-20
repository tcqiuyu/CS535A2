package bolt;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import lossy.LossyEntry;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import util.Utils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Qiu on 11/10/15.
 */
public class TweetHashtagCounterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private ConcurrentHashMap<String, LossyEntry> D = new ConcurrentHashMap<>();
    private Double s, epsilon;
    private Integer w;
    private Integer N = 0;
    private Integer b_current = 1;
    private int interval;
    private int i;

    public TweetHashtagCounterBolt(Double s, Integer w, int interval) {
        this.s = s;
        this.epsilon = 1.0 / w;
        this.w = w;
        this.interval = interval;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        try {
            if (Utils.isTickTuple(input)) {
                List<LossyEntry> output = OutputResult();
                Collections.sort(output);
                Collections.reverse(output);
                if (output.size() >= 100) {
                    output = output.subList(0, 100);
                }

//                System.out.println("-----------------------------------i=" + i + "----------------------------------------------");
//                for (LossyEntry anOutput : output) {
//                    System.out.println(anOutput.getElement() + ": f=" + anOutput.getFrequency() + ", delta=" + anOutput.getDelta());
//                }
//                System.out.println("---------------------------------------------------------------------------------");
                collector.emit(new Values(output));
                i = 0;
            } else {
                // Get incoming hashtag
                String hashtag = (String) input.getValue(0);
                LossyEntry incomingEntry = new LossyEntry(hashtag, 1, b_current - 1);
                this.LossyCounting(incomingEntry);
                collector.ack(input);
                i++;
            }
        } catch (Exception e) {
            collector.reportError(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag-count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config config = new Config();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, interval);
        return config;
    }

    public void LossyCounting(LossyEntry entry) {

        if (D.containsKey(entry.getElement())) {
            LossyEntry existedEntry = D.get(entry.getElement());
            existedEntry.setFrequency(existedEntry.getFrequency() + 1);
            D.put(existedEntry.getElement(), existedEntry);
        } else {
            D.put(entry.getElement(), entry);
        }

        N++;

//        System.out.println("N=" + N + ",w=" + w);
        if (N % w == 0) {
            Set<String> keySet = D.keySet();
            for (String hashtag : keySet) {
                LossyEntry entry2 = D.get(hashtag);
                if (entry2.getFrequency() + entry2.getDelta() <= b_current) {
//                    System.out.println("Remove item:" + entry2.getElement() + ", frequency=" + entry2.getFrequency()
//                            + ", delta=" + entry2.getDelta() + " b_current=" + b_current);
                    D.remove(entry2.getElement());
                }
//                System.out.println(b_current);
            }
            b_current++;
        }
    }

    public ArrayList<LossyEntry> OutputResult() {
        Set<String> keySet = D.keySet();
        ArrayList<LossyEntry> outputList = new ArrayList<>();

        for (String hashtag : keySet) {
            LossyEntry entry = D.get(hashtag);
            if (entry.getFrequency() < (s - epsilon) * N) {
                outputList.add(entry);
            }
        }
//        D = new ConcurrentHashMap<>();
//        b_current = 1;
        return outputList;
    }
}

package bolt;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import lossy.LossyEntry;
import util.Utils;

import java.io.FileWriter;
import java.util.*;

/**
 * Created by Qiu on 11/11/15.
 */
public class OutputResultBolt extends BaseBasicBolt {
    private int interval;
    private String outputPath;
    private FileWriter fileWriter;
    private HashMap<String, LossyEntry> aggregateTop100;
    private LinkedList<LossyEntry> outputTop100;

    public OutputResultBolt(int interval, String outputPath) {
        this.interval = interval;
        this.outputPath = outputPath;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config config = new Config();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, interval);
        return config;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
//        try {
//            fileWriter = new FileWriter(outputPath);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        aggregateTop100 = new HashMap<>();
        outputTop100 = new LinkedList<>();

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            if (Utils.isTickTuple(input)) {
                Set<String> hashtags= aggregateTop100.keySet();
                for (String hashtag : hashtags) {
                    outputTop100.add(aggregateTop100.get(hashtag));
                }
                Collections.sort(outputTop100);
                Collections.reverse(outputTop100);


                System.out.println("---------------------------------------------------------------------------------");
                for (LossyEntry anOutput : outputTop100) {
                    System.out.println(anOutput.getElement() + ": f=" + anOutput.getFrequency() + ", delta=" + anOutput.getDelta());
                }
                System.out.println("---------------------------------------------------------------------------------");

                aggregateTop100 = new HashMap<>();
                outputTop100 = new LinkedList<>();
            } else {
                List<LossyEntry> incomingTop100 = (List) input.getValue(0);
                for (LossyEntry entry : incomingTop100) {
                    mergeTop100(entry);
                }
            }

        } catch (Exception e) {
            collector.reportError(e);
        }

    }


    private void mergeTop100(LossyEntry entry) {
        if (aggregateTop100.containsKey(entry.getElement())) {
            LossyEntry entry1 = aggregateTop100.get(entry.getElement());
            Integer frequency = entry1.getFrequency();
            entry.setFrequency(entry.getFrequency() + frequency);
        }
//        System.out.println("Put item:" + entry.getElement() + ", frequency=" + entry.getFrequency()
//        + ", delta=" + entry.getDelta());
        aggregateTop100.put(entry.getElement(), entry);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }


}

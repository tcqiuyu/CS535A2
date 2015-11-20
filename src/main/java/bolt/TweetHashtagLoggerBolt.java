package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Tuple;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Created by Qiu on 11/12/15.
 */
public class TweetHashtagLoggerBolt extends BaseRichBolt {

    private FileWriter fileWriter;
    private String outputPath;
    private OutputCollector collector;

    public TweetHashtagLoggerBolt(String outputPath) {
        this.outputPath = outputPath;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            fileWriter = new FileWriter(outputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Status tweet = (Status) input.getValue(0);

        if (tweet.getHashtagEntities().length > 0) {
//            System.out.print("<"+tweet.getCreatedAt()+">");
            try {
                fileWriter.write("<" + tweet.getCreatedAt() + ">");
                for (HashtagEntity hashtag : tweet.getHashtagEntities()) {
//                System.out.print("<"+hashtag.getText()+">");
                    fileWriter.write("<" + hashtag.getText() + ">");
                }
//            System.out.print(":" + tweet.getText());
//                fileWriter.write(":" + tweet.getText() + "\n");
                fileWriter.write("\n");
//            System.out.println();
//            System.out.println("---------------------------------------------------------------------------------------");
                fileWriter.write("---------------------------------------------------------------------------------------\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        collector.ack(input);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}

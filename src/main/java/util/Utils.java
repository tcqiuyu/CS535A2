package util;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

/**
 * Created by Qiu on 11/15/15.
 */
public class Utils {

    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

}

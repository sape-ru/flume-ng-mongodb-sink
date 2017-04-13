package org.riderzen.flume.sink;

import com.mongodb.DBObject;
import org.apache.flume.Event;

public interface MongoEventSerializer {
    Event addHeaders(Event event);
    DBObject process(MongoSink conf, Event event) throws Exception;
}

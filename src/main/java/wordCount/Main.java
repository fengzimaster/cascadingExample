package wordCount;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;

import cascading.operation.Aggregator;
import cascading.operation.aggregator.Count;
import cascading.pipe.*;
import cascading.pipe.assembly.*;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.util.Properties;

public class Main {
    public static void main(String[] args){
        String inputpath = args[0];
        String outpath = args[1];
        Fields inputField = new Fields("cookie", "date");
        Fields outputField = new Fields("date", "count", "distinct_count");
        Tap source = new Hfs(new TextDelimited(inputField, ","), inputpath);
        Tap sink = new Hfs(new TextDelimited(outputField, ","), outpath);
        Pipe wordAssymble = new Pipe("cookieStatic");

        //sort
        Fields groupFields = new Fields("cookie", "date");
        Fields sortFields = new Fields("cookie", "date");
        wordAssymble = new GroupBy(wordAssymble,groupFields,sortFields);

        Aggregator count = new Count(new Fields("date_cookie_count"));
        wordAssymble = new Every(wordAssymble, new Fields("cookie"), count, new Fields("date","date_cookie_count"));

        //sum and count simultaneously

        Fields groupByDate = new Fields("date");
        SumBy sum = new SumBy(new Fields("date_cookie_count"), new Fields("count"), Long.class);
        CountBy distinctCount = new CountBy(new Fields("distinct_count"));
        wordAssymble = new AggregateBy(wordAssymble, groupByDate, sum, distinctCount);
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Main.class);
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);
        Flow flow=flowConnector.connect("cookieStatic", source, sink, wordAssymble);

        Cascade cascade = new CascadeConnector().connect(flow);
        cascade.complete();
    }
}

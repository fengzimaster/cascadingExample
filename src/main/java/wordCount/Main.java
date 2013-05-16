package wordCount;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;

import cascading.operation.Aggregator;
import cascading.operation.aggregator.Count;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.SumBy;
import cascading.property.AppProps;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

import cascading.tuple.Fields;

import java.util.Properties;


/**
 * Created with IntelliJ IDEA.
 * User: lifeng
 * Date: 5/15/13
 * Time: 5:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class Main {
    public static void main(String[] args){
        String inputpath=args[0];
        String outpath=args[1];
        //String outpath=args[2];
        Fields inputField=new Fields("cookie","date");
        Fields outputField=new Fields("date","count","distinct_count") ;
        // 从local file to hdfs file
        Tap resource=new Hfs(new TextDelimited(inputField,","),inputpath);
        Tap sink=new Hfs(new SequenceFile(outputField),outpath);
        Pipe wordAssymble=new Pipe("cookieStatic");
        //sort
        Fields groupFields=new Fields("cookie","date");
        Fields sortFields=new Fields("cookie","date");
        wordAssymble=new GroupBy(wordAssymble,groupFields,sortFields);

        Aggregator count=new Count(new Fields("date_cookie_count")) ;
        wordAssymble=new Every(wordAssymble,new Fields("cookie"),count,new Fields("date","date_cookie_count"));
//
        //sum and count simultaneously

        Fields groupByDate=new Fields("date");
        SumBy sum=new SumBy(new Fields("date_cookie_count"),new Fields("count"),Long.class);
        CountBy distinctCount= new CountBy(new Fields("distinct_count"));
        wordAssymble=new AggregateBy(wordAssymble,groupByDate,sum,distinctCount);
//        Identity id=new Identity(Fields.ARGS,Long.class) ;
//        wordAssymble=new Each(wordAssymble,new Fields("date_cookie_count"),id,Fields.REPLACE);
//        wordAssymble=new GroupBy(wordAssymble,new Fields("date"));
//        Sum sum=new Sum(new Fields("count"));
//        wordAssymble=new Every(wordAssymble,new Fields("date_cookie_count"),sum,new Fields("date","count"));
//
//        wordAssymble=new GroupBy(wordAssymble,new Fields("date","count"));
//        Count count2=new Count(new Fields("distinct"));
//        wordAssymble=new Every(wordAssymble,count2,new Fields("date","count","distinct"));

//        wordAssymble=new CountBy(wordAssymble,new Fields("date"),new Fields("distinct_count")) ;
//        wordAssymble=new SumBy(wordAssymble,new Fields("date"),new Fields("date_cookie_count"),new Fields("count"),Long.class);
//        Aggregator sum=new Sum(new Fields("count"));
//        wordAssymble=new Every(wordAssymble,new Fields("distinct_count"),sum,Fields.ALL);
       // wordAssymble=new Every(wordAssymble,new Fields("date"),count,Fields.REPLACE);
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Main.class);
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);
         Flow flow=flowConnector.connect("cookieStatic",resource,sink,wordAssymble);
//        Pipe transfer=new Pipe("transfer");
//        Flow importDataFlow=flowConnector.connect(localPagesSource,importedPages,transfer) ;
        Cascade cascade = new CascadeConnector().connect( flow );
        cascade.complete();
    }
}

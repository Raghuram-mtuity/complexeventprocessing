package com.cetana.cepprocessing;

import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.input.source.PassThroughSourceMapper;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.stream.input.source.SourceMapper;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.stream.InputStream;
import org.wso2.siddhi.query.api.execution.query.selection.Selector;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

import java.io.*;

public class SiddhiExample {

    public static void main(String[] args) throws InterruptedException {

        String siddhiApp2="";
        try{
            FileReader fileReader = new FileReader("/home/raghuram/git/complexeventprocessing/src/main/resources/siddhiquery");
            BufferedReader bufferedReader = new BufferedReader((fileReader));
            siddhiApp2 += bufferedReader.readLine();
            System.out.println(siddhiApp2);
        }catch (IOException e){
            System.out.println("File exception");
        }



        // Create Siddhi Application
        String siddhiApp = "define stream StockEventStream (symbol string, price float, volume long); " +
                " " +
                "@info(name = 'query1') " +
                "from StockEventStream#window.time(5 sec)  " +
                "select symbol, sum(price) as price, sum(volume) as volume " +
                "group by symbol " +
                "insert into AggregateStockStream ;";

        //String streams = "" +"@App:name('TestSiddhiApp')" +"@source(type='inMemory', topic='stock', @map(type='json')) " +"define stream FooStream (symbol string, price float, volume long); " +"define stream BarStream (symbol string, price float, volume long); ";
        //String query = "" +"from FooStream " +"select * " +"insert into BarStream; ";

        String sampleApp = "@source(type = 'kafka', topic.list = 'RawTopic', partition.no.list = '0', "  +
                "threading.option = 'single.thread', group.id = 'group', bootstrap.servers = 'localhost:9092'," +
                "@map(type = 'json', enclosing.element = '$', @attributes(object = 'identity.objectID', " +
                "tenant = 'identity.tenantID', domain= 'source.domain' ))) " +
                "define stream SweetProductionStream (object string, tenant string, domain string); " +

                "@sink(type = 'kafka', topic = 'EventTopicQueue', bootstrap.servers = 'localhost:9092', " +
                "partition.no = '0',@map(type = 'json'))  "+
                "define stream LowProductionAlertStream (object string, tenant string, domain string);" +

                "@info(name = 'query1') " +
                "from SweetProductionStream " +
                "select * " +
                "insert into LowProductionAlertStream; ";

        // Creating Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        /*Query query = Query.query();
        query.from(
                //InputStream.stream("StockEventStream").filter(Expression.function("time", Expression.value(5)))
                InputStream.stream("StockEventStream").window("time", Expression.Time.sec(5))

        );
        query.select(Selector.selector().select("symbol", Expression.variable("symbol")).
                select("price", Expression.function("sum", Expression.variable("price"))).
                select("volume", Expression.function("sum", Expression.variable("volume"))).
                groupBy(Expression.variable("symbol")));
        query.insertInto("AggregateStockStream");*/

        //SiddhiApp.siddhiApp("testing").defineStream(StreamDefinition.id("adding"));

        /*SiddhiApp siddhiApp1 = SiddhiApp.siddhiApp("test").
                defineStream(StreamDefinition.id("StockEventStream").
                        attribute("symbol", Attribute.Type.STRING).
                        attribute("price", Attribute.Type.FLOAT).
                        attribute("volume", Attribute.Type.LONG))
                .addQuery(query);*/

        //Generating runtime
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp2);

        //Adding callback to retrieve output events from query
        /*siddhiAppRuntime.addCallback("AggregateStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    System.out.println(event.toString());
                }

                EventPrinter.print(events);
            }
        });*/

        siddhiAppRuntime.addCallback("LowProductionAlertStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events){
                    System.out.println(event.toString());
                }
            }
        });


        //Retrieving input handler to push events into Siddhi
        //InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockEventStream");

        //Starting event processing
        siddhiAppRuntime.start();

        //Sending events to Siddhi
        /*inputHandler.send(new Object[]{"IBM", 100f, 100L});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"IBM", 200f, 300L});
        inputHandler.send(new Object[]{"WSO2", 60f, 200L});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"WSO2", 70f, 400L});
        inputHandler.send(new Object[]{"GOOG", 50f, 30L});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"IBM", 200f, 400L});
        Thread.sleep(2000);
        inputHandler.send(new Object[]{"WSO2", 70f, 50L});
        Thread.sleep(2000);
        inputHandler.send(new Object[]{"WSO2", 80f, 400L});
        inputHandler.send(new Object[]{"GOOG", 60f, 30L});
        Thread.sleep(1000);*/

        //Shutting down the runtime
        //siddhiAppRuntime.shutdown();

        //Shutting down Siddhi
        //siddhiManager.shutdown();



        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.6,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55.678,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2\",\n" +
                "         \"price\":55,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");
        InMemoryBroker.publish("stock", " {\n" +
                "      \"event\":{\n" +
                "         \"symbol\":\"WSO2@#$%^*\",\n" +
                "         \"price\":55,\n" +
                "         \"volume\":100\n" +
                "      }\n" +
                " }");

    }
}
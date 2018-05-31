package com.cetana.cepprocessing;

import com.alibaba.fastjson.JSONObject;
import kafka.utils.Json;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.stream.input.source.AttributeMapping;
import org.wso2.siddhi.core.stream.input.source.InputEventHandler;
import org.wso2.siddhi.core.stream.input.source.SourceMapper;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.List;

public class EventSourceMapper extends SourceMapper{

    StreamDefinition streamDefinition;
    OptionHolder optionHolder;
    List<AttributeMapping> list;
    ConfigReader configReader;
    SiddhiAppContext siddhiAppContext;

    public void readJSON(String json){
        try{
            //JSONObject yup = Json.parseFull(json);
        }catch (Throwable t){

        }

    }


    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder, List<AttributeMapping> list, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.streamDefinition = streamDefinition;
        this.optionHolder = optionHolder;
        this.list = list;
        this.configReader = configReader;
        this.siddhiAppContext = siddhiAppContext;

    }

    public Class[] getSupportedInputEventClasses() {
        return new Class[0];
    }

    protected void mapAndProcess(Object o, InputEventHandler inputEventHandler) throws InterruptedException {

    }

    protected boolean allowNullInTransportProperties() {
        return false;
    }
}

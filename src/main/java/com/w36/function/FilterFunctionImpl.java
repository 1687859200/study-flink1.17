package com.w36.function;

import com.w36.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;

public class FilterFunctionImpl implements FilterFunction<WaterSensor> {

    public String id;

    public FilterFunctionImpl(String id) {
        this.id = id;
    }

    @Override
    public boolean filter(WaterSensor waterSensor) throws Exception {
        return this.id.equals(waterSensor.getId());
    }
}

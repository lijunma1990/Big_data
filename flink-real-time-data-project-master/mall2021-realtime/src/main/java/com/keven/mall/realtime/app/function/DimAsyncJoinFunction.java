package com.keven.mall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @author KevenHe
 * @create 2022/1/21 18:34
 */
public interface DimAsyncJoinFunction<T> {

    String getKey(T input);

    void join(T input, JSONObject dimInfo) throws ParseException;

}

package com.keven.mall.realtime.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author KevenHe
 * @create 2022/1/22 18:58
 */
@Target(ElementType.FIELD)
@Retention(RUNTIME)
public @interface TransientSink {
}

package com.leon.cdc.annotations;

import org.springframework.stereotype.Component;

import java.lang.annotation.*;


@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
@Component
public @interface TableHandler {

    String tableName() default "";
}

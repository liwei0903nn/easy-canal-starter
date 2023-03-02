package com.leon.canal.annotations;

import java.lang.annotation.*;


@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface CanalHandler {

    String tableName() default "";

}

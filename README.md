```
# easy-canal
监听mysql binlog变动, 可以选择使用canal(需要提前部署canal服务端)或者flink cdc
```

使用方法:


1. pom 引入

```
        <dependency>
            <groupId>io.github.liwei0903nn</groupId>
            <artifactId>easy-canal</artifactId>
            <version>5.0.8-SNAPSHOP</version>
        </dependency>
```



2. 配置文件增加 canal 配置

```
easy-cdc:
  enable: true         #cdc开启
  type: flink          #cdc类型, 可选flink或者canal(需要提前部署canal服务端)
  flink:
    dbHost: localhost  #数据库地址 
    dbPort: 3306       #数据库端口
    dbUserName: root   #数据库用户
    dbPassword: root   #数据库密码
    databaseList:      #flink 需要监听的数据库列表
      - test
    tableList:         #flink 需要监听的表, 不能只写表名,  需要使用完整的 数据库.表名
      - test.student
  canal:
    canalServerHost: localhost   #canal服务端地址
    canalServerPort: 11111       #canal服务端端口
    destination: example         #canal instance
    
```

3. 增加对应的实体类和处理类(CommonHandler), 使用 TableHandler 标记对应的数据库表名, 在对应的函数完成自己的业务逻辑

数据库实体类
```
@Data
public class Student {
   
    private Long id;


    private String name;

}
```

数据库变更处理类
```
@TableHandler(tableName = "test.student")
@Slf4j
public class StudentHandler extends CommonHandler<Student> {

    @Override
    public boolean onInsert(Student newData) {  //新增
        log.info("student insert: {}", newData);
        return true;
    }

    @Override
    public boolean onUpdate(Student oldData, Student newData) { //修改
        log.info("student update: {}, {}", oldData, newData);
        return true;
    }

    @Override
    public boolean onDelete(Student data) {    //删除
        log.info("student delete: {}", data);
        return true;
    }
}
```

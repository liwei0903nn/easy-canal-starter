```
# easy-canal
基于spring boot 的 canal 客户端, 可以快速接入canal client。
```

使用方法:


1. pom 引入(依赖fastjson)

```
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
            <version>1.2.80</version>
        </dependency>

        <dependency>
            <groupId>io.github.liwei0903nn</groupId>
            <artifactId>easy-canal</artifactId>
            <version>1.0.0-SNAPSHOP</version>
            <exclusions>
                <exclusion>
                    <artifactId>fastjson</artifactId>
                    <groupId>com.alibaba</groupId>
                </exclusion>
            </exclusions>
        </dependency>
```



2. 配置文件增加 canal 配置

```
easy-canal:
  host: 10.0.193.3
  port: 11111 
  destination: example
  username: 
  password:
```

3. 继承 CommonHandler 类, 使用Component标记对应的数据库表名, 在对应的函数完成自己的业务逻辑

```
@Data
public class Hospital {
   
    private Long id;


    private String name;

}
```


```
@Component("testdb.hospital")  // 注意这里需要使用 数据库.表名
@Slf4j
public class HospitalHandler extends CommonHandler<Hospital> {


    // 新增
    @Override
    public boolean onInsert(Hospital newData) {
        return true;
    }

    /**
     * 修改
     *
     * @param oldData 修改前的数据
     * @param newData 修改后的数据
     * @return
     */

    @Override
    public boolean onUpdate(Hospital oldData, Hospital newData) {
        return true;
    }

    // 删除
    @Override
    public boolean onDelete(Hospital data) {
        return true;
    }


}
```

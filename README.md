# canal starter
> canal client scaffold：canal客户端脚手架
***
> 前导  
> Canal中间件整体架构服务器/客户端模式，建议部署canal相关部件时独立于业务之外，BU层部件订阅消息即可要注意硬件的网络IO、CPU性能配置(client)

### Usage guide
- Pom adds dependencies
```
#maven pom
<dependency>
    <groupId>com.dj.boot</groupId>
    <artifactId>canal-spring-boot-starter</artifactId>
    <version>0.0.1-RELEASE</version>
</dependency>
```
- Enable the client
```
# Add annotations to the main entry class
@EnableCanalClient
```
- Yml cfg
```
# eg
dj:
  canal:
    instances:
      example:
        host: 127.0.0.1
        port: 11111
        schema: scm_business_db
```
- code
```
# eg
@Slf4j
@Component
public class CanalSub implements MessageSubscriber {

    @Override
    public void watch(List<CommonMessage> commonMessages) {
        log.info("Sub::{}", commonMessages);
    }
}
```



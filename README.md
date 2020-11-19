# cdc-go

### 系统启动时需要的环境变量:  
`postgres_db` 配置文件所在数据库地址  
`prometheus_address`  prometheus 暴露端口号  

示例:
```.env
postgres_db=postgres://postgres:postgres@192.168.142.128/postgres
prometheus_address=:9527
```

### 需要的数据库表

`cdc.monitor`脚本如下：  

```sql
CREATE SCHEMA IF NOT exists cdc;
DROP TABLE IF EXISTS cdc.monitor;
CREATE TABLE cdc.monitor
(
    id        serial primary key,
    body_type text,
    body      json
);
```

### 具体配置文件
```json5
{
  "identity_id": 1, 
  "listen": {
    "database_type": "postgres", // 数据库类型
    "conn": "postgres://postgres:postgres@192.168.142.128/postgres?replication=database", //数据库连接字符串
    "slot": {
      "slotName": "test_demo1", // postgres 复制槽名称
      "temporary": true, // 是否临时槽
      "plugin": "wal2json", // wal 解码插件
      "plugin_args": [] // 插件参数
    }
  },
  "monitors": [
    {
      "table": "test", // 监听表
      "schema": "test", //监听schema
      "fields": ["name"], //监听字段
      "behavior": "", // 监听行为，如果为空则监听INSERT UPDATE DELETE 操作
      "description": ""
    }
  ],
  "rabbit": {
    "conn": "amqp://admin:admin@172.16.127.100:26174", // rabbitmq协议地址
    "queue": "cdc_mq_demo_one" // 队列名称
  }
}

```
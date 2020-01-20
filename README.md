# redis-migrate

## 一.系统简介
#### 功能描述

redis-migrate 用于在不同的redis集群之间进行数据的迁移。<br />

## 二.系统部署

#### 代码地址

```http://10.61.13.146:7990/scm/vis_vms/redis-migrate.git```

#### 可执行文件部署
服务器上新建目录
>mkdir -p /usr/bestv/vas/vas_root/redis-migrate <br/>
>mkdir -p /usr/bestv/vas/vas_root/redis-migrate/conf

上传配置文件
>修改项目根目录下的conf/redis-migrate.yml文件如下，修改完毕之后上传到服务器的/usr/bestv/vas/vas_root/redis-migrate/conf/目录

```
## redis-migration迁移配置 ##
---
source:
  # 这里配置源redis的地址
  # host: 39.100.103.74:12345
  host: 127.0.0.1:6379
destRedis:
  # 这里配置目标redis的地址
  host: 127.0.0.1:6379 
scanCount: 20000 ## 单次扫描的数目，使用默认即可
logFile : /var/log/redis-migrate.log ## 配置日志文件
...
```

上传可执行文件
>上传build目录中的可执行文件`redis-migrate`到服务器上的/usr/bestv/vas/vas_root/redis-migrate/目录

配置定时脚本定期执行，添加每天2点执行一次任务：
>crontab -e
>0 2 * * * nohup /usr/bestv/vas/vas_root/redis-migrate/redis-migrate &

日志查看路径
>tail -f /var/log/redis-migrate.log

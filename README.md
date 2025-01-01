# mini-etl mysql binlog模式实现被动数据抽取
迷你ETL，通过被动模式实现数据的跨网络抽取(http协议-数据加密)

# 实现逻辑
保持服务端和客户端数据库结构一致，开启服务端服务端监控mysql的binlog，当数据发生变动会自动产生数据消息存储到本地，客户端启动后会连接服务端来消费数据，网络使用http进行连接传输(受网络限制)，客户端会定期轮询服务端获取变化的数据对本地进行更新。

# 配置设置
mysql 增加配置：my.cnf
[mysqld]
log-bin=mysql-bin
server-id=1

重启mysql后执行是否开启为ON: SHOW VARIABLES LIKE 'log_bin';


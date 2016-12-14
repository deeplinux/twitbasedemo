# twitbasedemo

基于《HBase实战》的demo程序twitbase修改，适应hbase1.0.0的版本。该项目仅用于参考基础API的使用。

0. 针对于hbase1.0.0
1. 书中API的写法大部分已经Deprecated，所以改为适应hbase1.0.0的写法
2. 书中EndPoint类型的协处理器的用法已不可用，新的EndPoint需要用到谷歌的protobuf
3. 自定义过滤器也不可用，需要用到谷歌的protobuf
4. 增加System.setProperty("hadoop.home.dir", "hadoop-common-2.2.0-bin-master的绝对路径");使得可以在本地未安装hadoop的情况下，运行该程序，hadoop-common-2.2.0-bin-master可以在通过[这里](https://github.com/deeplinux/hadoop-common-2.2.0-bin.git)下载
5. 去除原项目内的shell脚本及传参的处理，可直接在IDE内运行
6. 添加hbase-site.xml，可配置连接的zk地址或其他配置



QT
==

Test for hbase secondary index performance.


执行查询:
  
    java -jar target/QT-0.1-jar-with-dependencies.jar

配置文件: src/main/resources/query.xml

每个index起一个查询线程，index内部各字段说明：

    pid: 项目名
    segment: 用户群
    bucket_num: 采几个桶(1-256)
    type: mysql或hbase
    group_by_attr: 细分属性，只查用户群此项为NA
    time: 连续查询次数

例如，查询国家是br的用户语言信息

    <index>
      <pid>sof-dsk</pid>
      <segment>{"nation":"br"}</segment>
      <bucket_num>256</bucket_num>
      <type>hbase</type>
      <group_by_attr>language</group_by_attr>
      <times>1</times>
    </index>


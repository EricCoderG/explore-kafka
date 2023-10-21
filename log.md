# 日志段：保存消息文件的对象是怎么实现的？

## 日志

Kafka日志在磁盘上的组织架构如下图所示：

![image-20231021163311422-20231021211842754](/Users/gengruilin/Desktop/Projects/explore-kafka/images/log/image-20231021163311422-20231021211842754.png)

日志是Kafka服务器端代码的重要组件之一，很多其他的核心组件都是以日志为基础的。

Kafka日志对象由多个日志段对象组成，而每个日志段对象会在磁盘上创建一组文件，包括消息日志文件（.log）、位移索引文件（.index）、时间戳索引文件（.timeindex）以及已中止（Aborted）事务的索引文件（.txnindex）。当然，如果你没有使用Kafka事务，已中止事务的索引文件是不会被创建出来的。图中的一串数字0是该日志段的起始位移值（Base Offset），也就是该日志段中所存的第一条消息的位移值。

一般情况下，一个Kafka主题有很多分区，每个分区就对应一个Log对象，在物理磁盘上则对应于一个子目录。比如你创建了一个双分区的主题test-topic，那么，Kafka在磁盘上会创建两个子目录：test-topic-0和test-topic-1。而在服务器端，这就是两个Log对象。每个子目录下存在多组日志段，也就是多组.log、.index、.timeindex文件组合，只不过文件名不同，因为每个日志段的起始位移不同。

## 日志段代码解析

日志段是Kafka保存消息的最小载体。

单纯查看官网对该参数的说明，我们不一定能够了解它的真实作用。下面介绍一个实际生产中的例子

> 大面积日志段同时间切分，导致瞬时打满磁盘I/O带宽。对此，所有人都束手无策，最终只能求助于日志段源码。
>
> 最后，我们在LogSegment的shouldRoll方法中找到了解决方案：设置Broker端参数log.roll.jitter.ms值大于0，即通过给日志段切分执行时间加一个扰动值的方式，来避免大量日志段在同一时刻执行切分动作，从而显著降低磁盘I/O。

### 代码位置

`org/apache/kafka/storage/internals/log/LogSegment.java`

![截屏2023-10-21 18.06.49](/Users/gengruilin/Desktop/Projects/explore-kafka/images/log/%E6%88%AA%E5%B1%8F2023-10-21%2018.06.49.png)

### 日志段类声明

![image-20231021205109799](/Users/gengruilin/Desktop/Projects/explore-kafka/images/log/image-20231021205109799.png)
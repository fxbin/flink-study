参考链接：

* [getting-started_1.9](https://ci.apache.org/projects/flink/flink-docs-release-1.9/getting-started/tutorials/local_setup.html)
* [flink-learning](https://github.com/zhisheng17/flink-learning)

## 时间语义

* Processing Time：事件被处理时机器的系统时间
* Event Time：事件自身的时间
* Ingestion Time：事件进入 Flink 的时间

### Processing Time

Processing Time 是指事件被处理时机器的系统时间。

> 如果我们 Flink Job 设置的时间策略是 Processing Time 的话，那么后面所有基于时间的操作（如时间窗口）都将会使用当时机器的系统时间。每小时 Processing Time 窗口将包括在系统时钟指示整个小时之间到达特定操作的所有事件。

> 例如，如果应用程序在上午 9:15 开始运行，则第一个每小时 Processing Time 窗口将包括在上午 9:15 到上午 10:00 之间处理的事件，下一个窗口将包括在上午 10:00 到 11:00 之间处理的事件。

> Processing Time 是最简单的 "Time" 概念，不需要流和机器之间的协调，它提供了最好的性能和最低的延迟。但是，在分布式和异步的环境下，Processing Time 不能提供确定性，因为它容易受到事件到达系统的速度（例如从消息队列）、事件在系统内操作流动的速度以及中断的影响。

### Event Time

> Event Time 是指事件发生的时间，一般就是数据本身携带的时间。这个时间通常是在事件到达 Flink 之前就确定的，并且可以从每个事件中获取到事件时间戳。在 Event Time 中，时间取决于数据，而跟其他没什么关系。Event Time 程序必须指定如何生成 Event Time 水印，这是表示 Event Time 进度的机制。

> 完美的说，无论事件什么时候到达或者其怎么排序，最后处理 Event Time 将产生完全一致和确定的结果。但是，除非事件按照已知顺序（事件产生的时间顺序）到达，否则处理 Event Time 时将会因为要等待一些无序事件而产生一些延迟。由于只能等待一段有限的时间，因此就难以保证处理 Event Time 将产生完全一致和确定的结果。
  
> 假设所有数据都已到达，Event Time 操作将按照预期运行，即使在处理无序事件、延迟事件、重新处理历史数据时也会产生正确且一致的结果。 例如，每小时事件时间窗口将包含带有落入该小时的事件时间戳的所有记录，不管它们到达的顺序如何（是否按照事件产生的时间）。
  
### Ingestion Time

> Ingestion Time 是事件进入 Flink 的时间。 在数据源操作处（进入 Flink source 时），每个事件将进入 Flink 时当时的时间作为时间戳，并且基于时间的操作（如时间窗口）会利用这个时间戳。

> Ingestion Time 在概念上位于 Event Time 和 Processing Time 之间。 与 Processing Time 相比，成本可能会高一点，但结果更可预测。因为 Ingestion Time 使用稳定的时间戳（只在进入 Flink 的时候分配一次），所以对事件的不同窗口操作将使用相同的时间戳（第一次分配的时间戳），而在 Processing Time 中，每个窗口操作符可以将事件分配给不同的窗口（基于机器系统时间和到达延迟）。
  
> 与 Event Time 相比，Ingestion Time 程序无法处理任何无序事件或延迟数据，但程序中不必指定如何生成水印。

`综述： 在 Flink 中，Ingestion Time 与 Event Time 非常相似，唯一区别就是 Ingestion Time 具有自动分配时间戳和自动生成水印功能。`

### 如何设置 Time 策略？
在创建完流运行环境的时候，然后就可以通过 env.setStreamTimeCharacteristic 设置时间策略：

```Java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

// 其他两种:
// env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
// env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
```

### 使用场景分析

> 一般来说在生产环境中将 Event Time 与 Processing Time 对比的比较多，这两个也是我们常用的策略，Ingestion Time 一般用的较少。

> 用 Processing Time 的场景大多是用户不关心事件时间，它只需要关心这个时间窗口要有数据进来，只要有数据进来了，我就可以对进来窗口中的数据进行一系列的计算操作，然后再将计算后的数据发往下游。

> 而用 Event Time 的场景一般是业务需求需要时间这个字段（比如购物时是要先有下单事件、再有支付事件；借贷事件的风控是需要依赖时间来做判断的；机器异常检测触发的告警也是要具体的异常事件的时间展示出来；商品广告及时精准推荐给用户依赖的就是用户在浏览商品的时间段/频率/时长等信息），只能根据事件时间来处理数据，而且还要从事件中获取到事件的时间。 
>
> 问题： 数据出现一定的乱序、延迟几分钟等
>
> 解决： Flink WaterMark机制

## 窗口（Window）

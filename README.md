flink 学习记录

> tips: 大部分内容源自zhisheng的博客, 在此仅用于学习记录

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

```java
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

> 举个栗子：
>
> 通传感器的示例：统计经过某红绿灯的汽车数量之和？
> 假设在一个红绿灯处，我们每隔 15 秒统计一次通过此红绿灯的汽车数量

> 可以把汽车的经过看成一个流，无穷的流，不断有汽车经过此红绿灯，因此无法统计总共的汽车数量。但是，我们可以换一种思路，每隔 15 秒，我们都将与上一次的结果进行 sum 操作（滑动聚合）

> 定义一个 Window（窗口），Window 的界限是 1 分钟，且每分钟内的数据互不干扰，因此也可以称为翻滚（不重合）窗口
> 这样就可以统计出每分钟的数量。。。60个window

> 再考虑一种情况，每 30 秒统计一次过去 1 分钟的汽车数量之和
> 数据重合，120个window

### Window 有什么作用？

> 通常来讲，Window 就是用来对一个无限的流设置一个有限的集合，在有界的数据集上进行操作的一种机制。Window 又可以分为基于时间（Time-based）的 Window 以及基于数量（Count-based）的 window。

### Flink 自带的 Window

Flink 在 KeyedStream（DataStream 的继承类） 中提供了下面几种 Window：

* 以时间驱动的 Time Window
* 以事件数量驱动的 Count Window
* 以会话间隔驱动的 Session Window

`由于某些特殊的需要，DataStream API 也提供了定制化的 Window 操作，供用户自定义 Window。`

#### Time Window 使用及源码分析

> Time Window 根据时间来聚合流数据。例如：一分钟的时间窗口就只会收集一分钟的元素，并在一分钟过后对窗口中的所有元素应用于下一个算子。

示例Code:

> 输入一个时间参数，这个时间参数可以利用 Time 这个类来控制，如果事前没指定 TimeCharacteristic 类型的话，则默认使用的是 ProcessingTime

```java
dataStream.keyBy(1)
    .timeWindow(Time.minutes(1)) //time Window 每分钟统计一次数量和
    .sum(1);
```

时间窗口的数据窗口聚合流程如下图所示

![img 时间窗口的数据窗口聚合流程图](image/时间窗口的数据窗口聚合流程.jpg)

在第一个窗口中（1 ～ 2 分钟）和为7、第二个窗口中（2 ～ 3 分钟）和为 12、第三个窗口中（3 ～ 4 分钟）和为 7、第四个窗口中（4 ～ 5 分钟）和为 19。

该 timeWindow 方法在 KeyedStream 中对应的源码如下：

```java
//时间窗口
public WindowedStream<T, KEY, TimeWindow> timeWindow(Time size) {
    if (environment.getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime) {
        return window(TumblingProcessingTimeWindows.of(size));
    } else {
        return window(TumblingEventTimeWindows.of(size));
    }
}
```

另外在 Time Window 中还支持滑动的时间窗口，比如定义了一个每 30s 滑动一次的 1 分钟时间窗口，它会每隔 30s 去统计过去一分钟窗口内的数据，同样使用也很简单，输入两个时间参数，如下：

```java
dataStream.keyBy(1)
    .timeWindow(Time.minutes(1), Time.seconds(30)) //sliding time Window 每隔 30s 统计过去一分钟的数量和
    .sum(1);
```

滑动时间窗口的数据聚合流程如下图所示：

![img 滑动时间窗口的数据聚合流程](image/滑动时间窗口的数据聚合流程图.jpg)

在该第一个时间窗口中（1 ～ 2 分钟）和为 7，第二个时间窗口中（1:30 ~ 2:30）和为 10，第三个时间窗口中（2 ~ 3 分钟）和为 12，第四个时间窗口中（2:30 ~ 3:30）和为 10，第五个时间窗口中（3 ~ 4 分钟）和为 7，第六个时间窗口中（3:30 ~ 4:30）和为 11，第七个时间窗口中（4 ~ 5 分钟）和为 19。

```java
//滑动时间窗口
public WindowedStream<T, KEY, TimeWindow> timeWindow(Time size, Time slide) {
    if (environment.getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime) {
        return window(SlidingProcessingTimeWindows.of(size, slide));
    } else {
        return window(SlidingEventTimeWindows.of(size, slide));
    }
}
```

#### Count Window 使用及源码分析

> Apache Flink 还提供计数窗口功能，如果计数窗口的值设置的为 3 ，那么将会在窗口中收集 3 个事件，并在添加第 3 个元素时才会计算窗口中所有事件的值。

示例Code: 

```java
dataStream.keyBy(1)
    .countWindow(3) //统计每 3 个元素的数量之和
    .sum(1);
```

计数窗口的数据窗口聚合流程如下图所示：

![img 计数窗口的数据窗口聚合流程](image/计数窗口的数据窗口聚合流程图.jpg)

该 countWindow 方法在 KeyedStream 中对应的源码如下：

```java
//计数窗口
public WindowedStream<T, KEY, GlobalWindow> countWindow(long size) {
    return window(GlobalWindows.create()).trigger(PurgingTrigger.of(CountTrigger.of(size)));
}
```

另外在 Count Window 中还支持滑动的计数窗口，比如定义了一个每 3 个事件滑动一次的 4 个事件的计数窗口，它会每隔 3 个事件去统计过去 4 个事件计数窗口内的数据，使用也很简单，输入两个 long 类型的参数，如下：

```java
dataStream.keyBy(1) 
    .countWindow(4, 3) //每隔 3 个元素统计过去 4 个元素的数量之和
    .sum(1);
```
   
滑动计数窗口的数据窗口聚合流程如下图所示：

![img 滑动计数窗口的数据窗口聚合流程](image/滑动计数窗口的数据窗口聚合流程.jpg)

该 countWindow 方法在 KeyedStream 中对应的源码如下：

```java
//滑动计数窗口
public WindowedStream<T, KEY, GlobalWindow> countWindow(long size, long slide) {
    return window(GlobalWindows.create()).evictor(CountEvictor.of(size)).trigger(CountTrigger.of(slide));
}
```

#### Session Window 使用及源码分析

> Apache Flink 还提供了会话窗口，是什么意思呢？使用该窗口的时候你可以传入一个时间参数（表示某种数据维持的会话持续时长），如果超过这个时间，就代表着超出会话时长。



```java
dataStream.keyBy(1)
    .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))//表示如果 5s 内没出现数据则认为超出会话时长，然后计算这个窗口的和
    .sum(1);
```

会话窗口的数据窗口聚合流程如下图所示:

![img 会话窗口的数据窗口聚合流程](image/会话窗口的数据窗口聚合流程.jpg)

该 Window 方法在 KeyedStream 中对应的源码如下：

```java
//提供自定义 Window
public <W extends Window> WindowedStream<T, KEY, W> window(WindowAssigner<? super T, W> assigner) {
    return new WindowedStream<>(this, assigner);
}
```

### 如何自定义 Window？

![img 实现 Window 的机制](image/实现Window的机制.png)

#### Window 源码定义

```java
public abstract class Window {
    //获取属于此窗口的最大时间戳
    public abstract long maxTimestamp();
}
```

> Window 类图

![img Window类图](image/Window类图.png)

> TimeWindow 源码定义如下:

```java
public class TimeWindow extends Window {
    //窗口开始时间
    private final long start;
    //窗口结束时间
    private final long end;
}
```

> GlobalWindow 源码定义如下：

```java
public class GlobalWindow extends Window {

    private static final GlobalWindow INSTANCE = new GlobalWindow();

    private GlobalWindow() { }
    //对外提供 get() 方法返回 GlobalWindow 实例，并且是个全局单例
    public static GlobalWindow get() {
        return INSTANCE;
    }
}
```

### Window 组件之 WindowAssigner 使用及源码分析

> 到达窗口操作符的元素被传递给 WindowAssigner。WindowAssigner 将元素分配给一个或多个窗口，可能会创建新的窗口。

> 窗口本身只是元素列表的标识符，它可能提供一些可选的元信息，例如 TimeWindow 中的开始和结束时间。注意，元素可以被添加到多个窗口，这也意味着一个元素可以同时在多个窗口存在。

> WindowAssigner 的代码定义：
```java
public abstract class WindowAssigner<T, W extends Window> implements Serializable {
    //分配数据到窗口并返回窗口集合
    public abstract Collection<W> assignWindows(T element, long timestamp, WindowAssignerContext context);
}
```

> WindowAssigner抽象类有如下实现类：

![img WindowAssigner抽象类的实现类](image/WindowAssigner%20实现类.png)

> WindowAssigner 实现类的作用介绍

| Assigner | 说明 |
| - | - |
| GlobalWindows | 所有数据都分配到同一个窗口(GlobalWindow) |
| TumblingProcessingTimeWindows | 基于处理时间的滚动窗口分配处理 |
| TumblingEventTimeWindows | 基于事件时间的滚动窗口分配处理 |
| SlidingEventTimeWindows | 基于事件时间的滑动窗口分配处理 |
| SlidingProcessingTimeWindows | 基于处理时间的滑动窗口分配处理 |
| MergingWindowAssigner | 一个抽象类，提供merge 窗口的方法 |
| EventTimeSessionWindows | 基于事件时间可Merge的会话窗口分配处理 |
| ProcessingTimeSessionWindows | 基于处理时间可Merge的会话窗口分配处理 |

> 实现规律：
* 1、定义好实现类的属性
* 2、根据定义的属性添加构造方法
* 3、重写 WindowAssigner 中的 assignWindows 等方法
* 4、定义其他的方法供外部调用

> TumblingEventTimeWindows 源码：

```java
public class TumblingEventTimeWindows extends WindowAssigner<Object, TimeWindow> {
    //定义属性
    private final long size;
    private final long offset;

    //构造方法
    protected TumblingEventTimeWindows(long size, long offset) {
        if (Math.abs(offset) >= size) {
            throw new IllegalArgumentException("TumblingEventTimeWindows parameters must satisfy abs(offset) < size");
        }
        this.size = size;
        this.offset = offset;
    }

    //重写 WindowAssigner 抽象类中的抽象方法 assignWindows
    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        //实现该 TumblingEventTimeWindows 中的具体逻辑
    }

    //其他方法，对外提供静态方法，供其他类调用
}
```

### Window 组件之 Trigger 使用及源码分析

> Trigger 表示触发器，每个窗口都拥有一个 Trigger（触发器），该 Trigger 决定何时计算和清除窗口。当先前注册的计时器超时时，将为插入窗口的每个元素调用触发器。在每个事件上，触发器都可以决定触发，即清除（删除窗口并丢弃其内容），或者启动并清除窗口。一个窗口可以被求值多次，并且在被清除之前一直存在。注意，在清除窗口之前，窗口将一直消耗内存。

> Trigger 源码

```java
public abstract class Trigger<T, W extends Window> implements Serializable {
    //当有数据进入到 Window 运算符就会触发该方法
    public abstract TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception;
    //当使用触发器上下文设置的处理时间计时器触发时调用
    public abstract TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception;
    //当使用触发器上下文设置的事件时间计时器触发时调用该方法
    public abstract TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception;
}
```

> TriggerResult 源码

```java
public enum TriggerResult {

    //不做任何操作
    CONTINUE(false, false),

    //处理并移除窗口中的数据
    FIRE_AND_PURGE(true, true),

    //处理窗口数据，窗口计算后不做清理
    FIRE(true, false),

    //清除窗口中的所有元素，并且在不计算窗口函数或不发出任何元素的情况下丢弃窗口
    PURGE(false, true);
}
```

> Trigger 抽象类的实现类

![img Trigger 抽象类的实现类](image/Trigger%20抽象类的实现类.png)

> Trigger 实现类的作用介绍

| Trigger | 说明 | 
| - | - |
| EventTimeTrigger | 当水印通过窗口末尾时触发的触发器 |
| ProcessingTimeTrigger | 当系统时间通过窗口末尾时触发的触发器 |
| DeltaTrigger | 一种基于DeltaFunction和阈值触发的触发器 |
| CountTrigger | 一旦窗口中的元素数量达到给定数量时就触发的触发器 |
| PurgingTrigger | 一种触发器，可以将任何触发器转换为清除触发器 |
| ContinuousProcessingTimeTrigger | 触发器根据给定的时间间隔连续触发，时间间隔依赖于Job所在机器的系统时间 |
| ContinuousEventTimeTrigger | 触发器根据给定的时间间隔连续触发，时间间隔依赖于水印时间戳 |
| NeverTrigger | 一个从来不触发的触发器，作为GlobalWindow的默认触发器 |

> 实现规律：

* 1、定义好实现类的属性

* 2、根据定义的属性添加构造方法

* 3、重写 Trigger 中的 onElement、onEventTime、onProcessingTime 等方法

* 4、定义其他的方法供外部调用

> CountTrigger 源码

```java
public class CountTrigger<W extends Window> extends Trigger<Object, W> {
    //定义属性
    private final long maxCount;

    private final ReducingStateDescriptor<Long> stateDesc = new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);
    //构造方法
    private CountTrigger(long maxCount) {
        this.maxCount = maxCount;
    }

    //重写抽象类 Trigger 中的抽象方法 
    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        //实现 CountTrigger 中的具体逻辑
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }
}
``` 

### Window 组件之 Evictor 使用及源码分析

> Evictor 表示驱逐者，它可以遍历窗口元素列表，并可以决定从列表的开头删除首先进入窗口的一些元素，然后其余的元素被赋给一个计算函数，如果没有定义 Evictor，触发器直接将所有窗口元素交给计算函数。

> Evictor 的源码

```java
public interface Evictor<T, W extends Window> extends Serializable {
    //在窗口函数之前调用该方法选择性地清除元素
    void evictBefore(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);
    //在窗口函数之后调用该方法选择性地清除元素
    void evictAfter(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);
}
```

> Evictor的实现类

![img  Evictor的实现类](image/Evictor的实现类.png)

> Evictor 实现类的作用介绍

| Evictor | 说明 |
| - | - |
| TimeEvictor | 元素可以在窗口中存在一段时间，老数据会被清除 |
| CountEvictor | 窗口中可以保存指定数量的数据，超过则会清除老的数据 |
| DeltaEvictor | 根据DeltaFunction 的实现和阈值来决定如何清理数据 |

> 实现规律：

* 1、定义好实现类的属性

* 2、根据定义的属性添加构造方法

* 3、重写 Evictor 中的 evictBefore 和 evictAfter 方法

* 4、定义关键的内部实现方法 evict，处理具体的逻辑

* 5、定义其他的方法供外部调用

> CountEvictor 的源码

```java
public class CountEvictor<W extends Window> implements Evictor<Object, W> {
    private static final long serialVersionUID = 1L;

    //定义属性
    private final long maxCount;
    private final boolean doEvictAfter;

    //构造方法
    private CountEvictor(long count, boolean doEvictAfter) {
        this.maxCount = count;
        this.doEvictAfter = doEvictAfter;
    }
    //构造方法
    private CountEvictor(long count) {
        this.maxCount = count;
        this.doEvictAfter = false;
    }

    //重写 Evictor 中的 evictBefore 方法
    @Override
    public void evictBefore(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext ctx) {
        if (!doEvictAfter) {
            //调用内部的关键实现方法 evict
            evict(elements, size, ctx);
        }
    }

    //重写 Evictor 中的 evictAfter 方法
    @Override
    public void evictAfter(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext ctx) {
        if (doEvictAfter) {
            //调用内部的关键实现方法 evict
            evict(elements, size, ctx);
        }
    }

    private void evict(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) {
        //内部的关键实现方法
    }

    //其他的方法
}
```

> Flink 自带的 Window（Time Window、Count Window、Session Window）, 最后都会调用 Window 方法，源码如下： 

```java
//提供自定义 Window
public <W extends Window> WindowedStream<T, KEY, W> window(WindowAssigner<? super T, W> assigner) {
    return new WindowedStream<>(this, assigner);
}

//构造一个 WindowedStream 实例
public WindowedStream(KeyedStream<T, K> input,
        WindowAssigner<? super T, W> windowAssigner) {
    this.input = input;
    this.windowAssigner = windowAssigner;
    //获取一个默认的 Trigger
    this.trigger = windowAssigner.getDefaultTrigger(input.getExecutionEnvironment());
}
```

>  Window 方法传入的参数是一个 WindowAssigner 对象, （可以利用Flink 现有的 WindowAssigner, 也可以自定义自己的WindowAssigner）, 然后再通过构造一个 WindowedStream 实例（在构造实例的会传入 WindowAssigner 和获取默认的 Trigger）来创建一个 Window。

> 滑动计数窗口，在调用 window 方法之后，还调用了 WindowedStream 的 evictor 和 trigger 方法，trigger 方法会覆盖掉你之前调用 Window 方法中默认的 trigger，如下：

```java
//滑动计数窗口
public WindowedStream<T, KEY, GlobalWindow> countWindow(long size, long slide) {
    return window(GlobalWindows.create()).evictor(CountEvictor.of(size)).trigger(CountTrigger.of(slide));
}

//trigger 方法
public WindowedStream<T, K, W> trigger(Trigger<? super T, ? super W> trigger) {
    if (windowAssigner instanceof MergingWindowAssigner && !trigger.canMerge()) {
        throw new UnsupportedOperationException("A merging window assigner cannot be used with a trigger that does not support merging.");
    }

    if (windowAssigner instanceof BaseAlignedWindowAssigner) {
        throw new UnsupportedOperationException("Cannot use a " + windowAssigner.getClass().getSimpleName() + " with a custom trigger.");
    }
    //覆盖之前的 trigger
    this.trigger = trigger;
    return this;
}
```

> Evictor 是可选的，但是 WindowAssigner 和 Trigger 是必须会有的，这种创建 Window 的方法充分利用了 KeyedStream 和 WindowedStream 的 API，再加上现有的 WindowAssigner、Trigger、Evictor, 就可以创建Window



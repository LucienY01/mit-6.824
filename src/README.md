# MIT 6.824 2021 Labs

## Lab 1: MapReduce
实现一个MapReduce分布式计算框架的精简版本。

代码在`mr`目录下。

## Lab 2: Raft
实现Raft共识算法。

代码在`raft`目录下。

测试结果保存在`/raft/test-output`目录下。其中`2D and final`是完成2D时对4个子Lab的测试结果。

### 遇到的难题
#### TestFigure8Unreliable2C：达成一致失败
TestFigure8Unreliable2C有0.2%的机率（连续测试500次会出现1次）失败：存在server不能在规定时间内达成一致。我一开始以为是我的raft实现的正确性出错了，一通debug后却发现另有原因。

该测试会以2/3的概率延迟RPC的回复，延迟时间随机分布在200ms到2200ms之间。该测试需要每个follower在经历长时间不同的网络分区后，将自己的log追上最终leader的log，即使实现了log不一致时的快速回退方法，follower也仍然需要多次回退和通信才能追上leader。

在此回退期间，如果：
1. leader对某个folower的连续几个heartbeat恰好都被随机延迟了足够长的时间
2. 因为开了大量的协程，调度器可能会长时间没有切换到heartbeat sender

以上两种情况都可能会导致这个follower会成为新的leader，上一个leader的回退会功亏一篑，新的leader需要重头对没有追上的folower进行回退（因为新的leader在一开始将nextIndex设置为其log长度），这种情况会导致测试超时，不能在其设定好的10秒时间内使得所有server达成一致。因为随机性的存在，只要该测试运行足够多次，总有出现失败的时候。

我观察到日志里有大量空闲时的heartbeat，而leader方面不处理heatbeat的回退问题。于是我在follower的heartbeat的handler中将快速回退改回了论文中的慢回退，避免了无意义的内存查找，这使得测试总时长平均降低了3秒左右。并且我将leader方面的快速回退查找从顺序查找改成了二分查找，这使得测试总时长平均降低了0.5秒左右。改进后，运算时间减少了，连续测试800次也没有再出现该错误，即出错率小于%0.125。

因为测试失败率较小，每次修改打印信息后要测试许久才能复现，所以解决这个问题还是有点费时的，我用了将近2到3天时间来解决这个问题。

#### TestFigure8Unreliable2C：apply error
TestFigure8Unreliable2C也有0.2%的几率（连续测试500次会出现1次）发生另一种错误：apply error，即某个结点在某个index提交了和其他所有结点不一样的cmd。经过不断的分析和debug后，发现问题在于AppendEntriesArgs的Entries字段放置了对log的切片，而golang的切片的更改对于其他共享底层数组的切片是可见的，如果AppendEntriesArgs在长时间后重传，其中的Entries字段很可能已经发生改变了。在某种极端情况下，这些改变了的Entries会被某个网络故障一段时间之后恢复的follower接受，导致该follower提交不一致的cmd。

golang切片的共享可变性是一个可能的出错的点，以后在使用切片时我需要多加留意。

#### 在一个term中出现两个leader
在2D的测试中，偶尔会出现这种情况。follower选举超时后，启动一个协程来转换为candidate、为自己投票、请求其他结点的投票。
该协程获取到锁时需要检查自己的currentTerm是否已经增加，currenterm增加了表示自己可能在获取到锁之前为其他结点投过票了。
我之前没有做这个检查，一个结点为其他结点投票后，又为自己投票，导致了这种错误。

## Lab 3: Fault-tolerant Key/Value Service
基于Raft实现一个分布式容错Key/Value存储系统。

代码在`kvraft`目录下。
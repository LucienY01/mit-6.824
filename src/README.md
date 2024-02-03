# MIT 6.824 2020 Labs

## Lab 2: Raft
测试结果保存在`/raft/test-output`目录下。

* `2A-test-output`：完成2A时对2A的测试结果。
* `2B-test-output`：完成2B时对2B的测试结果。
* `2C and final-test-output`：完成2C时对2A、2B、2C的测试结果。该目录下`TestFigure8Unreliable2C-100`是对`TestFigure8Unreliable2C`测试函数的100次连续测试结果，因为该测试函数存在一个低概率bug，难以复现，排查该bug需要耗费很多时间，所以用100次连续测试确保测试失败率低于1%。
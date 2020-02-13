# subscribe_demo

go_demo是对应的go语言实现，后续会补充java、C++、python等语言的实现，如果您有好的实现方式，请联系zhangyu25@kingsoft.com。如果您在使用过程中发现问题，也请联系zhangyu25@kingsoft.com，我们会在第一时间处理。

go_demo这个示例展示了如何使用数据订阅从DTS获取数据并且解析数据。
整个的流程包含了
1.用户进行参数配置；
2.使用原生的kafka consumer从DTS获取增量数据；
3.将获取到数据进行解析，从中获取对应的库名、表名、SQL语句、点位信息等。


使用的时候请替换：
config.User = ""
config.Passwd = ""
// kafka broker url
config.BrokerURL = ""
// kafka consumer group name
config.GroupID = ""
// topic to consume, partition is 0
config.Topic = ""
config.StartTime = ""
如果您希望订阅响应topic全部消息（即从订阅任务创建开始所有增量数据），config.StartTime则置为空；如果您希望从可选时间范围内某一时刻开始订阅数据，则将config.StartTime置为某一时刻值，时间格式为"2006-01-02 15:04:05"。
配置完成后，直接运行go run go_demo.go，即可订阅数据。您也可以根据自己的需要对订阅结果进行相应处理。

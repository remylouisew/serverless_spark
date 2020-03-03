from pyspark import SparkContext
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing (necessary for twitter data)
import json

#sc = SparkContext("k8s://https://kubernetes.default.svc.cluster.local:443", appName="PySparkShell")
ssc = StreamingContext(sc, 60)

kafkaStream = KafkaUtils.createStream(ssc, 'topics.yokicloud.com:9094', 'PySparkShell', {'topic-name':1})

parsed = kafkaStream.map(lambda v: json.loads(v[1]))

parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()

ssc.start()
ssc.awaitTermination()

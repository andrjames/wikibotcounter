from kafka import KafkaConsumer, TopicPartition
from json import loads
import json

client = "localhost:9092"
consumer = KafkaConsumer(
    'test1',
     bootstrap_servers=['localhost:29092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     consumer_timeout_ms=10000,
     value_deserializer=lambda x: loads(x.decode('utf-8')))
     
botlist = []
countlist = []

for message in consumer:
    i = 0
    tendies = message.value['user']
    str(tendies)
    doesExist = True
    length = len(botlist)
    if ('Bot' in tendies or 'bot' in tendies):
        if length == 0:
            botlist.append(tendies)
            countlist.append(1)
        else:
            while i < length:
                if tendies == botlist[i]:
                    countlist[i]+=1
                    doesExist = True
                    break
                else:
                    doesExist = False
                    i+=1
            
        if doesExist == False:
            botlist.append(tendies)
            countlist.append(1)

print('Bot Totals:')
length = len(botlist)
i = 0
while i < length:
    print(botlist[i] + ': ' + str(countlist[i]))
    i += 1
 
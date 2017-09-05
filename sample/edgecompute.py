#!/usr/bin/env python
#coding=utf8

import sys
import os
from aliyunsdkcore import client
from aliyunsdkiot.request.v20170420 import RegistDeviceRequest
from aliyunsdkiot.request.v20170420 import PubRequest
import base64
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

import time
from sample_common import MNSSampleCommon
from mns.account import Account
from mns.queue import *

productKey = 'L3vQSTANhcK'
topicName = '/L3vQSTANhcK/ls1043a_gw/data'
queueName = 'aliyun-iot-L3vQSTANhcK'

# Read the basic configurations from sample.cfg
# WARNING: Please do not hard code your accessId and accesskey in next line.
# (more information: https://yq.aliyun.com/articles/55947)
accessKeyId,accesskeySecret,endpoint,token = MNSSampleCommon.LoadConfig()

clt = client.AcsClient(accessKeyId, accesskeySecret, 'cn-shanghai')

# Init my_account, my_queue
my_account = Account(endpoint, accessKeyId, accesskeySecret, token)
queue_name = sys.argv[1] if len(sys.argv) > 1 else queueName
boolbase64 = False if len(sys.argv) > 2 and sys.argv[2].lower() == "false" else True
my_queue = my_account.get_queue(queue_name)
my_queue.set_encoding(boolbase64)

# Read and delete the message from queue until the queue is empty
# receive message uses long polling mode, specify the long loop time to 3 second through wait_seconds

# long polling parsing
# Return the message when there are messages
# When queue is empty, request will wait for 3 second on server side. 
# Request will return the message when mesages are wrote to this queue during this time.
# After 3 second, there is not yet message in queue, request will return 'MessageNotExist'
wait_seconds = 5
print "%sReceive And Delete Message From Queue%s\nQueueName:%s\nWaitSeconds:%s\n" % (10*"=", 10*"=", queue_name, wait_seconds)
while True:
	# Read the message
	try:
		recv_msg = my_queue.receive_message(wait_seconds)
		print "Received Message:" + recv_msg.message_body
	except MNSExceptionBase,e:
		if e.type == "QueueNotExist":
			print "Queue not exist, please create queue before receive message."
			sys.exit(0)
		elif e.type == "MessageNotExist":
			print "Queue is empty!, Retry it!"
		else:
			print "Receive Message Fail! Exception:%s\n" % e
		continue

	request = PubRequest.PubRequest()
	request.set_accept_format('json')
	request.set_ProductKey(productKey)
	request.set_TopicFullName(topicName)
	message = json.loads(recv_msg.message_body) #change the tring to dict
	messagebody = message['payload']
	decode_payload = base64.b64decode(messagebody)
	print decode_payload
	try:
		data_payload = json.loads(decode_payload)
		temp = data_payload['Temperature']
		if temp > '300':
			state_payload = '{"mystate":"on"}'
		else:
			state_payload = '{"mystate":"off"}'
		# add the codes to calculate the temperature
		request.set_MessageContent(base64.b64encode(state_payload))
		request.set_Qos(0)
		result = clt.do_action_with_exception(request)
		print 'result : ' + result
	except:
		print 'No JSON object could be decoded/detected'

	#delete this message
	try:
		my_queue.delete_message(recv_msg.receipt_handle)
		print "Delete Message Succeed!  ReceiptHandle:%s" % recv_msg.receipt_handle
	except MNSException,e:
		print "Delete Message Fail! Exception:%s\n" % e

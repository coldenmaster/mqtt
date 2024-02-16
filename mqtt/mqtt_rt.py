 
# from frappe.model.base_document import BaseDocument
from paho.mqtt import client as mqtt
import requests

import frappe

from bbl_api.utils import print_clear, print_red


class MqttRoute(object):
    
    default_topic = 'testtopic/#'
    default_point = 'http://127.0.0.1:8000/api/method/mqtt.mqtt_rt.hdl'
    # route_map = {'testtopic/#': 'http://127.0.0.1:8000/api/method/mqtt.mqtt_rt.hdl'}
    route_map = {default_topic: default_point}

    # todo 注意开单独的线程打开连接，监听（client.loop_start()不阻塞）
    def __init__(self, mqtt_host, mqtt_port, mqtt_keepalive):
        super(MqttRoute, self).__init__()
        client = mqtt.Client()
        self.client = client
        client.on_connect = self.on_connect
        client.on_message = self.on_message
        client.on_publish = self.on_publish
        client.on_subscribe = self.on_subscribe
        client.connect(mqtt_host, mqtt_port, mqtt_keepalive)  # 600为keepalive的时间间隔
        client.subscribe('testtopic/#')
        client.loop_start()     # 保持连接
        # client.loop_forever()   # 保持连接(阻塞)

    '''
    关于loop()
        loop_start()是启用一个(新)进程保持loop()的重复调用，就不需要定期心跳了，对应的有loop_stop()。
        loop_forever()用来保持无穷阻塞调用loop()(阻塞)
    '''
 
    def on_connect(self, client, userdata, flags, rc):
        print_clear("Connected with result code: " + str(rc))
        # 订阅
 
 
    def on_message(self, client, userdata, msg):
        print_clear("mqtt on_message: " + str(msg))
        # data = {
        #     'topic': msg.topic,
        #     'payload': str(msg.payload.decode('utf-8')),
        #     'userdate': str(userdata)
        #     }
        # topic = msg.topic
        # if not topic in self.route_map.keys():
        #     topic = topic.split('/')[0] + "/#"
        # if not topic in self.route_map.keys():
        #     topic = self.default_topic
        # url = self.route_map.get(topic, self.default_point)
        # print(f"mqtt on_message, topic:{msg.topic} sendto:{url}")
        # rt = self.send_request(url, data)
        # print(f"on_message, url return:{rt}")

    def send_request(self, url, data):
        try:
            requests.post(url, data=data,timeout=1)
        except Exception as e:
            print(e)

    
 
    #   订阅回调
    def on_subscribe(self, client, userdata, mid, granted_qos):
        print_clear("On Subscribed: qos = %d" % granted_qos)
 
    #   取消订阅回调
    def on_unsubscribe(self, client, userdata, mid):
        # print("取消订阅")
        print_clear("On unSubscribed: qos = %d" % mid)
        pass
 
    #   发布消息回调
    def on_publish(self, client, userdata, mid):
        # print("发布消息")
        print_clear("On onPublish: mid = %d" % mid)
        pass
 
    #   断开链接回调
    def on_disconnect(self, client, userdata, rc):
        # print_clear("断开链接")
        # todo 注意断线重连
        # client.loop_stop()
        print_clear("Unexpected disconnection rc = " + str(rc))
        pass

    def register_topic_rt(self, topic, url):
        if topic in self.route_map.keys():
            print_clear(f"register_topic_rt(), topic: {topic} already exists")
            return
        self.client.subscribe(topic)
        self.route_map[topic] = url
        # self.client.publish(topic.replace('/#', ''), f'frappe register topic:{topic} to url:{url}')
        print_clear(f"register_topic_rt(), topic: {topic}, url: {url}")
        # print_clear(f"mqtt_route_map: {self.route_map}")


# http://127.0.0.1:8000/api/method/mqtt.mqtt_rt.hdl
@frappe.whitelist(allow_guest=True)
def hdl(*args, **kwargs):
    print_clear(f"mqtt 缺省控制器, mqtt_rt.hdl(), kwargs: { kwargs }")
    return "hdl ok"


mqtt_route = MqttRoute("223.75.192.139", 1883, 600)
bbl_mqtt_client = mqtt_route.client
# bbl_mqtt_client.publish(mqtt_route.default_topic.replace('/#', ''), "import mqtt OK")
print_red(f"import mqtt OK, mqtt is {mqtt_route}")
 
 
if __name__ == '__main__':
    # mr = MqttRoute("223.75.192.139", 1883, 600)
    # client = mr.client
    # mr.register_topic_rt('testtopic', 'http://127.0.0.1:8000/api/method/mqtt.mqtt_rt.hdl')
    # client.publish('testtopic', 'msg dsb250')
    # client.loop_forever()     
    # mqtt_route.register_topic_rt('testtopic', 'http://127.0.0.1:8000/api/method/mqtt.mqtt_rt.hdl')
    import time
    # mqtt_route.register_topic_rt('testtopic/#', '/250bb')
    for i in range(6):
        bbl_mqtt_client.publish('testtopic/2', f'msg dsb250 {i}')
        time.sleep(10)
    # bbl_mqtt_client.loop_forever()

    print("mqtt_rt.py, end")


    
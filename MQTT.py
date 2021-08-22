import asyncio
from gmqtt import Client
import uvloop


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
STOP = asyncio.Event()
TOPIC = 'user_48853d2a/TEST'


def on_connect(client, flags, rc, properties):
    client.subscribe(TOPIC, qos=0)
    print('Connected')


def on_message(client, topic, payload, qos, properties):
    print('topic:', topic, 'message:', payload.decode(), 'properties:', properties)
    ask_exit()


def on_disconnect(client, packet, exc=None):
    print('Disconnected')


def on_subscribe(client, mid, qos, properties):
    print('SUBSCRIBED')


def ask_exit(*args):
    STOP.set()


async def run(data):
    client = Client("user_48853d2a_test")

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe

    client.set_auth_credentials(username="user_48853d2a", password="pass_617a76a8")
    await client.connect(host='srv1.clusterfly.ru', port=9124)

    client.publish(TOPIC, data, qos=1)

    await STOP.wait()
    # await client.disconnect()







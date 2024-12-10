import sys
import json
from datetime import datetime
from twisted.internet import defer, reactor
from afkak.client import KafkaClient
from afkak.consumer import Consumer
from afkak.producer import Producer
from afkak.common import OFFSET_EARLIEST, PRODUCER_ACK_ALL_REPLICAS, PRODUCER_ACK_LOCAL_WRITE


KAFKA_BROKER_ADDR = "localhost:9092"


@defer.inlineCallbacks
def produce(data_file):
    print('starting producer')
    kafka_client = KafkaClient(KAFKA_BROKER_ADDR, timeout=50000)
    producer = Producer(kafka_client)
    date_now = f">>>>>>> {str(datetime.now())}"

    # r0 = yield producer.send_messages("foo", msgs=[msg.encode()])
    # print(f"producer | msg 'foo0' | resp - {r0} ")

    # r1 = yield producer.send_messages("foo", msgs=[b"foo0"])
    # print(f"producer | msg 'foo0' | resp - {r1} ")

    # r2 = yield producer.send_messages("foo", msgs=[b"foo1", b"foo2"])
    # print(f"producer | msg 'foo1, foo2' | resp - {r2} ")

    print(f"Loading data from {data_file}")
    with open(data_file) as f:
        data = json.load(f)
    # msg = {"foo": "ram", "timestamp": date_now}
    msg = json.dumps(data).encode()
    print(f"sending msg - {msg}")

    r0 = yield producer.send_messages("foo", msgs=[msg])
    print(f"producer | topic 'foo' | resp - {r0} ")



def producer_final(result):
    print(f"producer final result - {result}")
    reactor.stop()


def main(data_file):
    df = produce(data_file)
    df.addBoth(producer_final)


if __name__ == "__main__":
    try:
        data_file = sys.argv[1]
    except IndexError:
        data_file = "foo.json"

    reactor.callLater(1, main,data_file)
    reactor.run()
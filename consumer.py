import sys
import json
from twisted.internet import defer, task, reactor
from afkak.client import KafkaClient
from afkak.consumer import OFFSET_LATEST, OFFSET_EARLIEST, Consumer

TOPIC = "foo"
KAFKA_BROKER_ADDR = "localhost:9092"


def processor(consumer, message_list):
    for msg in message_list:
        print(f"Got msg - {msg}")
        print(msg[0], msg[1], msg[2])
        m = msg.message.value.decode()
        m = json.loads(m)
        print('received')
        # print(m)


def consumer_closed(msg):
    print(f"consumer closed - {msg}")


def consumer_failed(msg):
    print(f"consumer failed - {msg}")


def stop_consumers(kafka_client, consumers):
    print("\n")
    print("Time is up, stopping consumers...")
    d = defer.gatherResults([c.shutdown() for c in consumers])
    d.addCallback(lambda result: kafka_client.close())
    return d



@defer.inlineCallbacks
def consume():
    kafka_client = KafkaClient(KAFKA_BROKER_ADDR, reactor=reactor, timeout=500000)
    e = True
    while e:
        yield kafka_client.load_metadata_for_topics(TOPIC)
        e = kafka_client.metadata_error_for_topic(TOPIC)
        if e:
            print(f"error getting metadata for topic {TOPIC}: {str(e)} (will retry)")


    partitions = kafka_client.topic_partitions[TOPIC]
    consumers = [Consumer(kafka_client, TOPIC, partition, processor) for partition in partitions]
    print(f"consumer - {consumers}")

    # for consumer in consumers:
    #     consumer.start(OFFSET_LATEST).addCallbacks(consumer_closed, consumer_failed)

    yield defer.gatherResults(
        [c.start(OFFSET_LATEST).addCallbacks(consumer_closed, consumer_failed) for c in consumers] + [task.deferLater(reactor,1800.0, stop_consumers, kafka_client, consumers)]
    )



def consumer_final(result):
    print(f"consumer final result - {result}")


def main():
    df = consume()
    df.addBoth(consumer_final)


if __name__ == "__main__":
    reactor.callLater(1, main)
    reactor.run()
# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from kombu import Connection, Exchange, Queue
from pprint import pprint


conf = {
    'user': 'streamer',
    'pass': 'streamer',
    'port': 2765,
    'gh_torrent_exchange_name': 'ght-streams',
    'queue_name': 'swh_queue',
    # http://ghtorrent.org/streaming.html  : {evt|ent}.{entity|event}.action
    'routing_key': 'evt.*.insert',
}

# works with fake event generator
# conf = {
#     'user': 'guest',
#     'pass': 'guest',
#     'port': 5672,
#     'gh_torrent_exchange_name': 'ght-streams',
#     'queue_name': 'fake-events',
#     'routing_key': 'something',

# }

server_url = 'amqp://%s:%s@localhost:%s//' % (
    conf['user'], conf['pass'], conf['port'])

exchange = Exchange(conf['gh_torrent_exchange_name'],
                    'topic', durable=True)
test_queue = Queue(conf['queue_name'],
                   exchange=exchange,
                   routing_key=conf['routing_key'],
                   auto_delete=True)


def process_message(body, message):
    print('#### body')
    pprint(body)
    print('#### message')
    pprint(message)
    message.ack()


with Connection(server_url) as conn:
    # produce
    # producer = conn.Producer(serializer='json')
    # producer.publish('hello',
    #                  exchange=media_exchange, routing_key='test',
    #                  declare=[test_queue])

    # the declare above, makes sure the test queue is declared
    # so that the messages can be delivered.
    # It's a best practice in Kombu to have both publishers and
    # consumers declare the queue. You can also declare the
    # queue manually using:
    #     test_queue(conn).declare()

    print('Connection established')

    # consume
    with conn.Consumer(test_queue, callbacks=[process_message],
                       auto_declare=True) as consumer:
        # Process messages and handle events on all channels
        while True:
            conn.drain_events()

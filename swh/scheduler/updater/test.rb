require 'bunny'

# apt install ruby-bunny
conn = Bunny.new(:host => '127.0.0.1', :port => 2765,
                 :username => 'streamer', :password => 'streamer')
conn.start
ch  = conn.create_channel
exchange = ch.topic('ght-streams', :durable => true)
queue_name = 'swh_ghtorrent_queue'
q = ch.queue(queue_name, :auto_delete => true)
# q.bind(exchange, :routing_key => 'evt.*.*')
# q.bind(exchange, :routing_key => 'evt.push.insert')
q.bind(exchange, :routing_key => '*.*.*')
# q.bind(exchange, :routing_key => 'evt.create|delete|public|push.insert')
q.subscribe do |delivery_info, properties, payload|
  puts "#{delivery_info.routing_key}: #{payload}"
end

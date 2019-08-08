using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace KafkaTest {
    public class ViewLiveStreamWorkflow : IDisposable {
        IProducer<string, string> _producer;
        IConsumer<string, string> _consumer;
        const string ViewLiveStreamRequestTopic = "viewlivestream_request";
        const string ViewLiveStreamResultTopic = "viewlivestream_result_cameraname";
        string _bootstrapServers;
        public ViewLiveStreamWorkflow(string bootstrapServers) {
            _bootstrapServers = bootstrapServers;
            var producer_config = new ProducerConfig { BootstrapServers = bootstrapServers };
            var consumer_config = new ConsumerConfig
            {
                GroupId = "group1",
                BootstrapServers = bootstrapServers,
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };
            _producer = new ProducerBuilder<string, string>(producer_config).Build();
            _consumer = new ConsumerBuilder<string, string>(consumer_config).Build();
            Task.Run(()=> SubscribeResult());
        }

        public async Task Start(string cameraId)
        {
            await ProduceRequest(cameraId);
            
        }
        private async Task ProduceRequest(string cameraId)
        {
            var request = new ViewLiveStreamRequest { CameraId = cameraId, ClientId = "A01" };
            var value = JsonConvert.SerializeObject(request);
            await _producer.ProduceAsync(ViewLiveStreamRequestTopic, new Message<string, string> { Value = value });
            Console.WriteLine($"Request sent:{value}");
        }

        private void SubscribeResult()
        {
            _consumer.Subscribe(ViewLiveStreamResultTopic);
            
            try
            {
                while(true)
                {
                    try
                    {
                        var cr = _consumer.Consume();
                        var result = JsonConvert.DeserializeObject<ViewLiveStreamResult>(cr.Value);
                        _consumer.Commit();
                        Console.WriteLine($"Received result '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                        
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                };
                
            }
            catch (OperationCanceledException)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                _consumer.Close();
            }
        }

        

        public void Dispose()
        {
            _producer.Dispose();
            _consumer.Dispose();
        }
    }
}

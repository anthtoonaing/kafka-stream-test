using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace KafkaTest {
    public class UpdateCameraProcessor : IDisposable {
        IProducer<string, string> _producer;
        IConsumer<string, string> _consumer;
        private string _inputTopic;
        private string _outputTopic;
        public UpdateCameraProcessor(string bootstrapServers, string inputTopic)
        {
            _inputTopic = inputTopic;
            
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
            _consumer.Subscribe(_inputTopic);
        }

        public void Dispose()
        {
            _producer.Dispose();
            _consumer.Dispose();
        }

        public async Task Run(string cameraId, string cameraName)
        {
            var request = new CameraRequest
            {
                CameraId = cameraId,
                CameraName = cameraName
            };
            var requestJson = JsonConvert.SerializeObject(request);
            await _producer.ProduceAsync(_inputTopic, new Message<string, string> { Key= cameraId, Value = requestJson });
            var message = _consumer.Consume();
            Console.WriteLine(message.Value);

        }

        
    }
}

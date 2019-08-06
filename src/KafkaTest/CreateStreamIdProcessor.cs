using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace KafkaTest {
    public class CreateStreamIdProcessor : IDisposable {
        IProducer<string, string> _producer;
        IConsumer<string, string> _consumer;
        private string _inputTopic;
        private string _outputTopic;
        public CreateStreamIdProcessor(string bootstrapServers, string inputTopic, string outputTopic)
        {
            _inputTopic = inputTopic;
            _outputTopic = outputTopic;
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

        }

        public void Dispose()
        {
            _producer.Dispose();
            _consumer.Dispose();
        }

        public async Task Run()
        {
            _consumer.Subscribe(_inputTopic);
            await Task.Run(RunInternal);
            
        }

        private async Task RunInternal()
        {
            while(true)
            {
                var message = _consumer.Consume();
                var input = JsonConvert.DeserializeObject<CreateStreamIdRequest>(message.Value);
                var streamId = $"{input.CameraId}-{DateTime.Now.Ticks}";
                Console.WriteLine($"CreateStreamIdProcessor Input {streamId}");
                var value = new CreateStreamIdResult { CameraId = input.CameraId, StreamId = streamId, RequestId=input.RequestId };
                var valueJson = JsonConvert.SerializeObject(value);

                await _producer.ProduceAsync(_outputTopic, new Message<string, string> { Value = valueJson });
                _consumer.Commit();
                Console.WriteLine($"CreateStreamIdProcessor Output {valueJson}");
            };
            
        }
    }
}

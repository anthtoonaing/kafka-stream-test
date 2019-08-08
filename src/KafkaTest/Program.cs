using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaTest {
    class Program {
        public static string _bootstrapServers = "192.168.0.178:9092";
        static string topic = "users";
        static ViewLiveStreamWorkflow _workflow = new ViewLiveStreamWorkflow(_bootstrapServers);
        public static void Main()
        {

            //Task.Run(()=>Consume("g1", "g1-c1"));
            //Task.Run(() => Consume("g1", "g1-c2"));
            //Task.Run(() => Consume("g1", "g1-c3"));
            //Task.Run(() => Consume("g1", "g1-c4"));
            //Task.Run(() => Consume("g1", "g1-c5"));
            var cameraProcess = new UpdateCameraProcessor(_bootstrapServers, "camera");
            var _ = RunCreateStreamProcessor();
            var __ = RunGetVmsStreamProcessor();
            do
            {
                Console.Write("CameraId(,CameraName):");
                var cameraInfo = Console.ReadLine();
                var split = cameraInfo.Split(',');
                var id = split[0];
                if(split.Length > 1)
                {
                    var name = split[1];
                    cameraProcess.Run(id, name).Wait();
                }
                RunWorkflow(id).Wait();
                
                Console.ReadLine();
            } while (true);
            
            Console.ReadLine();
        }

        public static async Task UpdateCamera() {
            
            
        }
        private static async Task RunWorkflow(string cameraId)
        {
            Console.WriteLine("Starting View Live Stream Request ...");
            await _workflow.Start(cameraId);
            //Console.WriteLine("View Live Stream Requested");
        }

        private static async Task RunCreateStreamProcessor()
        {
            Console.WriteLine("Starting Create Stream Processor ...");
            using (var processor = new CreateStreamIdProcessor(_bootstrapServers, "createstreamid_request", "createstreamid_result"))
            {
                await processor.Run();
            }
            Console.WriteLine("Create Stream Processor Completed!");
        }

        private static async Task RunGetVmsStreamProcessor()
        {
            Console.WriteLine("Starting GetVmsStreamProcessor ...");
            using (var processor = new GetVmsStreamProcessor(_bootstrapServers, "getvmsstream_request", "getvmsstream_result"))
            {
                await processor.Run();
            }
            Console.WriteLine("GetVmsStreamProcessor Completed!");
        }

        public static async Task ProduceUser()
        {
            var config = new ProducerConfig { BootstrapServers = _bootstrapServers };

            // If serializers are not specified, default serializers from
            // `Confluent.Kafka.Serializers` will be automatically used where
            // available. Note: by default strings are encoded as UTF8.
            using (var p = new ProducerBuilder<string, string>(config).Build())
            {
                try
                {
                    var time = DateTime.Now.Second;
                    //var dr = await p.ProduceAsync(new TopicPartition(topic, new Partition(key % 5)), new Message<string, string> {  Value = "test" });
                    var userId = $"User_{ time}";
                    var value = $"{DateTime.Now.Ticks},M,R-{time % 6},{userId}";
                    var dr = await p.ProduceAsync("users_p1", new Message<string, string> {Key=userId, Value =  value});
                    Console.WriteLine($"Delivered {time} =>'{dr.Value}' to '{dr.TopicPartitionOffset}'");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }

        }
        public static async Task ProducePageView()
        {
            var config = new ProducerConfig { BootstrapServers = _bootstrapServers };

            // If serializers are not specified, default serializers from
            // `Confluent.Kafka.Serializers` will be automatically used where
            // available. Note: by default strings are encoded as UTF8.
            using (var p = new ProducerBuilder<string, string>(config).Build())
            {
                try
                {
                    var key = DateTime.Now.Millisecond;
                    var time = DateTime.Now.Second;
                    //var dr = await p.ProduceAsync(new TopicPartition(topic, new Partition(key % 5)), new Message<string, string> {  Value = "test" });
                    var userId = $"User_{ time}";
                   // var value = $"{DateTime.Now.Ticks},M,R-{time % 6},{userId}";
                    //var dr = await p.ProduceAsync(new TopicPartition(topic, new Partition(key % 5)), new Message<string, string> {  Value = "test" });
                    var dr = await p.ProduceAsync("pageviews_p2", new Message<string, string> { Value = $"{DateTime.Now.Ticks},{userId},Page_{key}" });
                    Console.WriteLine($"Delivered {key} =>'{dr.Value}' to '{dr.TopicPartitionOffset}'");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }

        }

        public static async Task Produce()
        {
            var config = new ProducerConfig { BootstrapServers = _bootstrapServers };

            // If serializers are not specified, default serializers from
            // `Confluent.Kafka.Serializers` will be automatically used where
            // available. Note: by default strings are encoded as UTF8.
            using (var p = new ProducerBuilder<string, string>(config).Build())
            {
                try
                {
                    var key = DateTime.Now.Millisecond;
                    //var dr = await p.ProduceAsync(new TopicPartition(topic, new Partition(key % 5)), new Message<string, string> {  Value = "test" });
                    var dr = await p.ProduceAsync(topic, new Message<string, string> { Value = "hello,world" });
                    Console.WriteLine($"Delivered {key} =>'{dr.Value}' to '{dr.TopicPartitionOffset}'");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }

        }

        public static void Consume(string groupId, string consumerId)
        {
            var conf = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = _bootstrapServers,
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe("users");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"{consumerId}:Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                            var result = c.Commit();
                            //Console.WriteLine($"{consumerId}:Commited message '{result.}' at: '{cr.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"{consumerId}:Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
        }
    }
}

using Confluent.Kafka;
using System;

namespace TesteKafkaConsumer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var groupId = Guid.NewGuid().ToString();

            Console.WriteLine($"Initializing");

            var conf = new ConsumerConfig
            { 
                GroupId = "test-consumer-group-" + groupId,
                BootstrapServers = "dkafka-yield01:9092",
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = false,
                AutoCommitIntervalMs = 0
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe("mkt-data-topic");

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume();

                            Console.WriteLine($"Consumed message '{cr.Value + " - delivered - " + DateTime.Now.Millisecond.ToString()}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    c.Close();
                }
            }
        }
    }
}

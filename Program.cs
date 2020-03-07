using System;
using Confluent.Kafka;
using System.Threading;

namespace TesteKafkaConsumer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine($"Initializing");

            var conf = new ConsumerConfig
            { 
                GroupId = "test-consumer-group",
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
                            Console.WriteLine($"Consumed message '{cr.Value}'.");
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

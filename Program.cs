using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;

namespace TesteKafkaConsumer
{
    public class Program
    {
        private const string InstrumentInfoTopic = "mkt-data-topic-instrument";
        private const string QuotesInfoTopic = "mkt-data-topic-quote";
        private const string QuotesInfoTopicSpecifc = "mkt-data-topic-quote-";
        private const string BookInfoTopic = "mkt-data-topic-book";

        public static void Main(string[] args)
        {
            IEnumerable<string> assets = new string[] { "PETR4", "VALE3", "ITUB4", "GGBR4", "SANB11", "MGLU3", "CAML3", "BIDI4", "BBDC4", "BOVA11", "BPAC11", "ITSA3", "WINZ17" };
            string[] topics = new string[assets.Count()];
            int i = 0;

            foreach (var item in assets)
            {
                topics[i] = QuotesInfoTopicSpecifc + item;
                i++;
            }
                    


            var groupId = Guid.NewGuid().ToString();

            Console.WriteLine($"Initializing");

            var conf = new ConsumerConfig
            { 
                GroupId = "test-consumer-group-" + groupId,
                //BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = false,
                AutoCommitIntervalMs = 0
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe(topics);

                var teste = c.Subscription;

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

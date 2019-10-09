// Copyright 2016-2018 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Benchmark
{
    public static class BenchmarkConsumer
    {
        public static void BenchmarkConsumerImpl(string bootstrapServers, string topic, long firstMessageOffset, int nMessages, int nTests, int nHeaders)
        {
            //create new consumer group id to restart from the beginning
            var consumerGroupId = $"benchmark-consumer-group-{Guid.NewGuid().ToString()}";
            List<Task> runningTasks = new List<Task>();

            var consumerConfig = new ConsumerConfig
            {
                GroupId = consumerGroupId,
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                ConsumeResultFields = nHeaders == 0 ? "none" : "headers",
                AutoCommitIntervalMs = 10000,
            };

            using (var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build())
            {
                for (var j = 0; j < nTests; j += 1)
                {
                    Console.WriteLine($"{DateTime.UtcNow} -- Start {consumer.Name}-{consumerGroupId} consuming from {topic}");

                    consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic, 0, firstMessageOffset) });

                    // consume 1 message before starting the timer to avoid including potential one-off delays.
                    var record = consumer.Consume(TimeSpan.FromSeconds(1));

                    long startTime = DateTime.Now.Ticks;

                    var cnt = 0;
                    const int outputMessageAtX = 10000;
                    int outputCounter = 0;

                    while (cnt < nMessages - 1)
                    {
                        record = consumer.Consume(TimeSpan.FromMilliseconds(500));
                        if (record != null)
                        {
                            cnt += 1;
                            outputCounter += 1;

                            runningTasks.Add(Task.Run(() =>
                            {
                                ProcessRecord(record);
                            }));

                            if (outputCounter >= outputMessageAtX)
                            {
                                Console.WriteLine($"{DateTime.UtcNow} -- At offset {record.Offset} from {topic}. Output every {outputCounter} messages.");

                                outputCounter = 0;
                            }
                        }
                    }

                    Task.WhenAll(runningTasks);

                    var duration = DateTime.Now.Ticks - startTime;
                    var msg1 = $"Consumed {cnt} messages in {duration / 10000.0:F0}ms";
                    var msg2 = $"{(cnt) / (duration / 10000.0):F0}k msg/s";
                    Console.WriteLine($"{DateTime.UtcNow} -- End {consumer.Name}-{consumerGroupId} consuming from {topic}");

                    Console.WriteLine(msg1);
                    Console.WriteLine(msg2);
                }
            }
        }

        private static bool ProcessRecord(ConsumeResult<Ignore, Ignore> record)
        {
            return (record != null);
        }

        public static void Consume(string bootstrapServers, string topic, long firstMessageOffset, int nMessages, int nHeaders, int nTests)
            => BenchmarkConsumerImpl(bootstrapServers, topic, firstMessageOffset, nMessages, nTests, nHeaders);
    }
}
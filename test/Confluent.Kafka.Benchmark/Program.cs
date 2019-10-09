// Copyright 2016-2017 Confluent Inc.
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

namespace Confluent.Kafka.Benchmark
{
    /// <summary>
    /// Sample command line calls:
    /// First one sends 0 as first paramter, which tells system to not publish messages to the tmr.prd125.dev.kafkaconsumerbenchmark on the broker list.
    /// Second one sends 1 as first paramter, which tells system to publish messages to the tmr.prd125.dev.kafkaconsumerbenchmark on the broker list.
    /// 0 "kf0001.cf.prd349.dev3.datasciences.tmcs:9092,kf0002.cf.prd349.dev3.datasciences.tmcs:9092,kf0003.cf.prd349.dev3.datasciences.tmcs:9092,kf0004.cf.prd349.dev3.datasciences.tmcs:9092,kf0005.cf.prd349.dev3.datasciences.tmcs:9092,kf0006.cf.prd349.dev3.datasciences.tmcs:9092" "tmr.prd125.dev.kafkaconsumerbenchmark" 2
    /// 1 "kf0001.cf.prd349.dev3.datasciences.tmcs:9092,kf0002.cf.prd349.dev3.datasciences.tmcs:9092,kf0003.cf.prd349.dev3.datasciences.tmcs:9092,kf0004.cf.prd349.dev3.datasciences.tmcs:9092,kf0005.cf.prd349.dev3.datasciences.tmcs:9092,kf0006.cf.prd349.dev3.datasciences.tmcs:9092" "tmr.prd125.dev.kafkaconsumerbenchmark" 2
    /// </summary>
    public class Program
    {
        public static void Main(string[] args)
        {
            if (args.Length != 3 && args.Length != 4)
            {
                Console.WriteLine($"Usage: .. [runProduce 1=true;0=false] <broker,broker..> <topic> [header-count]");
                return;
            }

            bool runProduce = false;

            if (Int32.TryParse(args[0], out Int32 firstParmValue) == true)
            {
                runProduce = (firstParmValue == 1);
            }
            var bootstrapServers = args[1];
            var topic = args[2];
            var headerCount = 0;
            if (args.Length > 3)
            {
                headerCount = int.Parse(args[3]);
            }

            const int NUMBER_OF_MESSAGES = 500000;
            const int NUMBER_OF_TESTS = 1;
            long firstMessageOffset = 1;

            if (runProduce == true)
            {
                BenchmarkProducer.TaskProduce(bootstrapServers, topic, NUMBER_OF_MESSAGES, headerCount, NUMBER_OF_TESTS);
                firstMessageOffset = BenchmarkProducer.DeliveryHandlerProduce(bootstrapServers, topic, NUMBER_OF_MESSAGES, headerCount, NUMBER_OF_TESTS);
            }

            BenchmarkConsumer.Consume(bootstrapServers, topic, firstMessageOffset, NUMBER_OF_MESSAGES, headerCount, NUMBER_OF_TESTS);
        }
    }
}
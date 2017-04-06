using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BoltSDK;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Newtonsoft.Json.Linq;

namespace ConsoleApp2
{
    class Program
    {
        static void Main(string[] args)
        {
            BoltSDK.WorkerTools b = new BoltSDK.WorkerTools();
            //b.CreateChannel();
            var ip = "localhost";
            var factory = new ConnectionFactory() { Uri = "amqp://guest:guest@" + ip };
            using (var connection = factory.CreateConnection())
            using (var channel = WorkerTools.CreateChannel(connection))
            {
                var consumer = WorkerTools.ConsumeCommand("parseKeywords", channel);
                WorkerTools.ConsumeCommand("fetchPage", channel, consumer);

                while (true)
                {
                    var ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();

                    var payload = WorkerTools.StartWork(ea);

                    try
                    {
                        if (ea.RoutingKey == "parseKeywords")
                        {
                            var html = payload.SelectToken("return_value.html").ToString();

                            Console.WriteLine(" [x] Received HTML: " + html);

                            var kw = new List<string>();
                            kw.Add("abc");
                            kw.Add("123");

                            payload["return_value"]["keywords"] = new JArray(kw.ToArray());
                            Console.WriteLine(" [x] Keywords Parsed: " + payload["return_value"]["keywords"].ToString());
                        }
                        else if (ea.RoutingKey == "fetchPage")
                        {
                            payload["return_value"]["html"] = "C# Worker Page Content";
                        }
                    }
                    catch (Exception err)
                    {
                        Console.WriteLine("[x] Error: " + err.ToString());
                    }
                    finally
                    {
                        WorkerTools.FinishWork(channel, ea, payload);
                    }
                };

            }
        }
    }
}

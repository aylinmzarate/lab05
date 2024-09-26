using System.Text.Json;
using Confluent.Kafka;

var config = new ConsumerConfig { BootstrapServers = "localhost:29092", GroupId = "reader" };
var consumer = new ConsumerBuilder<Null, string>(config).Build();
consumer.Subscribe("books");
Console.WriteLine("Subscribed to books.");

while (true) {
    var consumed = consumer.Consume();
    var book = JsonSerializer.Deserialize<Book>(consumed.Message.Value)!;
    Console.WriteLine($"Consumed book '{book.Title}' by {book.Author} ({book.Year})");
}
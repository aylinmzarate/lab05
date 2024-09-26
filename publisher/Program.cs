using System.Text.Json;
using Confluent.Kafka;

var config = new ProducerConfig { BootstrapServers = "localhost:29092" };
var producer = new ProducerBuilder<Null, string>(config).Build();

while (true) {
    var book = Novels.Random();
    var message = new Message<Null, string> { Value = JsonSerializer.Serialize(book) };
    await producer.ProduceAsync("books", message);
    Console.WriteLine($"Produced book '{book.Title}' by {book.Author} ({book.Year})");
    await Task.Delay(1000);
}
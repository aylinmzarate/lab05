public record Book(string Title, string Author, int Year);

public static class Novels {
    public static Book TheTrial = new("The Trial", "Franz Kafka", 1925);
    public static Book TheCastle = new("The Castle", "Franz Kafka", 1926);
    public static Book Amerika = new("Amerika", "Franz Kafka", 1927);
    public static Book NineteenEightyFour = new("1984", "George Orwell", 1949);
    public static Book Plague = new("The Plague", "Albert Camus", 1947);

    public static Book[] All = [TheTrial, TheCastle, Amerika, NineteenEightyFour, Plague];

    public static Book Random() {
        var random = new Random();
        return All[random.Next(All.Length)];
    }
}
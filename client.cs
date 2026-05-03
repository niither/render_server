using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

class Program
{
    static readonly HttpClient Http = new();
    static string ServerUrl = "https://render-server-6j0m.onrender.com";

    static string ClientId = "";
    static int NextFrom = 0;

    class ChatMessage
    {
        public string From { get; set; } = "";
        public string Text { get; set; } = "";
        public bool IsSystem { get; set; }
    }

    static async Task Main()
    {
        Console.OutputEncoding = Encoding.UTF8;

        await Connect();

        Console.WriteLine("Введіть повідомлення та натисніть Enter. Ctrl+C для виходу.\n");

        // Запускаємо polling у фоні
        using var cts = new CancellationTokenSource();
        var pollTask = Task.Run(() => ListenLoop(cts.Token));

        // Головний потік: читаємо введення та надсилаємо повідомлення
        Console.CancelKeyPress += async (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
            await Disconnect();
            Environment.Exit(0);
        };

        while (true)
        {
            string? line = Console.ReadLine();
            if (line == null) break;
            if (string.IsNullOrWhiteSpace(line)) continue;
            await SendMessage(line);
        }

        cts.Cancel();
        await Disconnect();
    }

    // POST /connect
    static async Task Connect()
    {
        while (true)
        {
            try
            {
                var body = new { clientId = Guid.NewGuid().ToString("N")[..8] };
                var response = await PostJson("/connect", body);

                ClientId = response.GetProperty("clientId").GetString()!;
                NextFrom = response.GetProperty("fromIndex").GetInt32();

                Console.WriteLine($"Підключено! Ваш ID: {ClientId}");
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Не вдалося підключитися: {ex.Message}");
                Console.Write("Спробувати ще раз? (Enter = так, будь-що = ні): ");
                var ans = Console.ReadLine();
                if (ans != "") Environment.Exit(1);
            }
        }
    }

    // POST /disconnect
    static async Task Disconnect()
    {
        try
        {
            await PostJson("/disconnect", new { clientId = ClientId });
            Console.WriteLine("Відключено від сервера.");
        }
        catch { /* ігноруємо помилки при виході */ }
    }

    // POST /send
    static async Task SendMessage(string text)
    {
        try
        {
            await PostJson("/send", new { clientId = ClientId, text });
        }
        catch (Exception ex)
        {
            PrintSystem($"Помилка відправки: {ex.Message}");
        }
    }

    // GET /poll  — виконується кожні 1 секунду
    static async Task ListenLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var url = $"{ServerUrl}/events?clientId={ClientId}";
                using var stream = await Http.GetStreamAsync(url, ct);
                using var reader = new System.IO.StreamReader(stream);

                while (!ct.IsCancellationRequested)
                {
                    var line = await reader.ReadLineAsync(ct);
                    if (line == null) break; // server closed connection
                    if (!line.StartsWith("data: ")) continue;

                    var json = line["data: ".Length..];
                    var msg = JsonSerializer.Deserialize<ChatMessage>(json,
                        new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                    if (msg == null) continue;

                    if (msg.IsSystem)
                        PrintSystem(msg.Text);
                    else
                        PrintMessage(msg.From, msg.Text);
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                PrintSystem($"З'єднання перервано: {ex.Message}. Перепідключення...");
                await Task.Delay(2000, ct);
                await Connect();
            }
        }
    }

    // --- Вивід у консоль ---

    static void PrintMessage(string from, string text)
    {
        var prev = Console.ForegroundColor;
        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.Write($"[{from}]: ");
        Console.ForegroundColor = prev;
        Console.WriteLine(text);
    }

    static void PrintSystem(string text)
    {
        var prev = Console.ForegroundColor;
        Console.ForegroundColor = ConsoleColor.DarkGray;
        Console.WriteLine($"*** {text}");
        Console.ForegroundColor = prev;
    }

    // --- HTTP хелпери ---

    static async Task<JsonElement> PostJson(string path, object body)
    {
        var content = new StringContent(
            JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");
        var response = await Http.PostAsync(ServerUrl + path, content);
        var json = await response.Content.ReadAsStringAsync();
        return JsonDocument.Parse(json).RootElement;
    }
}

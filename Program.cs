using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

class Program
{
    // Всі повідомлення чату (індекс = ID повідомлення)
    static readonly List<ChatMessage> Messages = new();
    static readonly object MessagesLock = new();

    // Підключені клієнти: clientId -> час останнього запиту
    static readonly ConcurrentDictionary<string, DateTime> Clients = new();

    static async Task Main()
    {
        string port = Environment.GetEnvironmentVariable("PORT") ?? "5000";
        string url = $"http://+:{port}/";

        var listener = new HttpListener();
        listener.Prefixes.Add(url);
        listener.Start();
        Console.WriteLine($"Сервер запущено на порту {port}");
        Console.WriteLine("Ендпоінти:");
        Console.WriteLine("  POST /connect              -> реєстрація клієнта");
        Console.WriteLine("  POST /disconnect           -> відключення клієнта");
        Console.WriteLine("  POST /send                 -> надіслати повідомлення");
        Console.WriteLine("  GET  /poll?clientId=&from= -> отримати нові повідомлення");
        Console.WriteLine();

        // Фонове завдання: прибираємо клієнтів, які давно не опитували сервер
        _ = Task.Run(CleanupLoop);

        while (true)
        {
            var context = await listener.GetContextAsync();
            _ = Task.Run(() => HandleRequest(context));
        }
    }

    static async Task HandleRequest(HttpListenerContext context)
    {
        var req = context.Request;
        var res = context.Response;
        res.ContentType = "application/json; charset=utf-8";

        try
        {
            string path = req.Url?.AbsolutePath ?? "/";

            if (req.HttpMethod == "POST" && path == "/connect") await HandleConnect(req, res);
            else if (req.HttpMethod == "POST" && path == "/disconnect") await HandleDisconnect(req, res);
            else if (req.HttpMethod == "POST" && path == "/send") await HandleSend(req, res);
            else if (req.HttpMethod == "GET" && path == "/poll") await HandlePoll(req, res);
            else
            {
                res.StatusCode = 404;
                await WriteJson(res, new { error = "Не знайдено" });
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[!] Помилка: {ex.Message}");
            res.StatusCode = 500;
            await WriteJson(res, new { error = ex.Message });
        }
    }

    // POST /connect
    // Body: { "clientId": "abc" }
    static async Task HandleConnect(HttpListenerRequest req, HttpListenerResponse res)
    {
        var body = await ReadJson<ConnectRequest>(req);
        string id = body?.ClientId ?? Guid.NewGuid().ToString("N")[..8];

        Clients[id] = DateTime.UtcNow;

        string sysMsg = $"Клієнт {id} приєднався до чату";
        AddSystemMessage(sysMsg);
        Console.WriteLine($"[+] {sysMsg}. Усього клієнтів: {Clients.Count}");

        // Повертаємо ID та поточний індекс, щоб клієнт не читав старі повідомлення
        int fromIndex;
        lock (MessagesLock) fromIndex = Messages.Count;

        await WriteJson(res, new { clientId = id, fromIndex });
    }

    // POST /disconnect
    // Body: { "clientId": "abc" }
    static async Task HandleDisconnect(HttpListenerRequest req, HttpListenerResponse res)
    {
        var body = await ReadJson<ConnectRequest>(req);
        if (body?.ClientId != null && Clients.TryRemove(body.ClientId, out _))
        {
            string sysMsg = $"Клієнт {body.ClientId} покинув чат";
            AddSystemMessage(sysMsg);
            Console.WriteLine($"[-] {sysMsg}. Усього клієнтів: {Clients.Count}");
        }
        await WriteJson(res, new { ok = true });
    }

    // POST /send
    // Body: { "clientId": "abc", "text": "Привіт!" }
    static async Task HandleSend(HttpListenerRequest req, HttpListenerResponse res)
    {
        var body = await ReadJson<SendRequest>(req);

        if (string.IsNullOrWhiteSpace(body?.ClientId) || string.IsNullOrWhiteSpace(body?.Text))
        {
            res.StatusCode = 400;
            await WriteJson(res, new { error = "clientId та text обов'язкові" });
            return;
        }

        if (!Clients.ContainsKey(body.ClientId))
        {
            res.StatusCode = 403;
            await WriteJson(res, new { error = "Спочатку підключіться через /connect" });
            return;
        }

        Clients[body.ClientId] = DateTime.UtcNow;
        AddMessage(body.ClientId, body.Text);
        Console.WriteLine($"[{body.ClientId}]: {body.Text}");

        await WriteJson(res, new { ok = true });
    }

    // GET /poll?clientId=abc&from=5
    // Повертає всі повідомлення починаючи з індексу from
    static async Task HandlePoll(HttpListenerRequest req, HttpListenerResponse res)
    {
        string? clientId = req.QueryString["clientId"];
        string? fromStr = req.QueryString["from"];

        if (string.IsNullOrWhiteSpace(clientId) || !Clients.ContainsKey(clientId))
        {
            res.StatusCode = 403;
            await WriteJson(res, new { error = "Невідомий клієнт" });
            return;
        }

        Clients[clientId] = DateTime.UtcNow;

        int from = int.TryParse(fromStr, out int f) ? f : 0;

        List<ChatMessage> newMessages;
        int nextFrom;
        lock (MessagesLock)
        {
            from = Math.Max(0, Math.Min(from, Messages.Count));
            newMessages = Messages.GetRange(from, Messages.Count - from);
            nextFrom = Messages.Count;
        }

        await WriteJson(res, new { messages = newMessages, nextFrom });
    }

    // Прибираємо клієнтів, які не опитували сервер довше 15 секунд
    static async Task CleanupLoop()
    {
        while (true)
        {
            await Task.Delay(10_000);
            var cutoff = DateTime.UtcNow.AddSeconds(-15);
            foreach (var (id, lastSeen) in Clients)
            {
                if (lastSeen < cutoff && Clients.TryRemove(id, out _))
                {
                    string sysMsg = $"Клієнт {id} відключився (таймаут)";
                    AddSystemMessage(sysMsg);
                    Console.WriteLine($"[~] {sysMsg}. Усього клієнтів: {Clients.Count}");
                }
            }
        }
    }

    static void AddMessage(string clientId, string text)
    {
        lock (MessagesLock)
            Messages.Add(new ChatMessage { From = clientId, Text = text, IsSystem = false });
    }

    static void AddSystemMessage(string text)
    {
        lock (MessagesLock)
            Messages.Add(new ChatMessage { From = "server", Text = text, IsSystem = true });
    }

    static async Task WriteJson(HttpListenerResponse res, object obj)
    {
        var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(obj));
        res.ContentLength64 = bytes.Length;
        await res.OutputStream.WriteAsync(bytes);
        res.Close();
    }

    static async Task<T?> ReadJson<T>(HttpListenerRequest req)
    {
        using var reader = new StreamReader(req.InputStream, Encoding.UTF8);
        var json = await reader.ReadToEndAsync();
        return JsonSerializer.Deserialize<T>(json,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
    }
}

class ChatMessage
{
    public string From { get; set; } = "";
    public string Text { get; set; } = "";
    public bool IsSystem { get; set; }
}

class ConnectRequest { public string? ClientId { get; set; } }
class SendRequest { public string? ClientId { get; set; } public string? Text { get; set; } }
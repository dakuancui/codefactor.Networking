using System.Net;
using codefactor;

int port = 1111;
if (args.Length > 0)
    port = int.Parse(args[0]);

Console.WriteLine($"TCP server port: {port}");

Console.WriteLine();

var server = new ChatServer(IPAddress.Any, port);

// Start the server
Console.Write("Server starting...");
server.Start();
Console.WriteLine("Done!");

Console.WriteLine("Press Enter to stop the server or '!' to restart the server...");


// Perform text input
//for (; ; )
//{
//    string line = Console.ReadLine();
//    if (string.IsNullOrEmpty(line))
//        break;

//    // Restart the server
//    if (line == "!")
//    {
//        Console.Write("Server restarting...");
//        server.Restart();
//        Console.WriteLine("Done!");
//        continue;
//    }

//    // Multicast admin message to all sessions
//    line = "(admin) " + line;
//    server.Multicast(line);

//    // Stop the server
//    Console.Write("Server stopping...");
//    server.Stop();
//    Console.WriteLine("Done!");


//}

var stop = false;
while (!stop) {
    string line = Console.ReadLine();
    if (string.IsNullOrEmpty(line))
        break;

    // Restart the server
    if (line == "!")
    {
        Console.Write("Server restarting...");
        server.Restart();
        Console.WriteLine("Done!");
        continue;
    }

    // Multicast admin message to all sessions
    line = "(admin) " + line;
    server.Multicast(line);

    // Stop the server
    Console.Write("Server stopping...");
    server.Stop();
    stop = true;
    Console.WriteLine("Done!");
}

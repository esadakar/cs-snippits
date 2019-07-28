using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

class TestClass
{
    static void Main(string[] args)
    {
        init();
        Thread.Sleep(30000);

    }

    static void init()
    {
        Cache.Upsert("5", "5", 5);
        Cache.Upsert("4", "4", 4);
        Cache.Upsert("3", "3", 3);
        Cache.Upsert("6", "6", 6);
        Cache.Upsert("2", "2", 2);
        Cache.Upsert("1", "1", 1);
        Cache.Upsert("8", "8", 8);
        Cache.Upsert("15", "15", 15);

    }
}


public static class Cache
{

    static int running = 0;

    class TtlNode 
    {
        public long Utime;
        public string Key {get; set;}
    }

    static object _lock = new Object();
    static Dictionary<string, string> dict = new Dictionary<string, string>();  
    static List<TtlNode> nodes = new List<TtlNode>();

    public static void Upsert(string key, string val, int ttl = 0)
    {
        lock(_lock)
        {
            string value;
            if (dict.TryGetValue(key, out value))
                value = val;
            else {
                dict.Add(key, val);
            }
        if (ttl > 0)
            addTtl(key, ttl);
        }
    }

    public static string Read(string key)
    {
        string ret;
        return dict.TryGetValue(key, out ret) ? ret : null;
    }

    static void DebugPrint()
    {
        nodes.ForEach(x => {
            Console.WriteLine(x.Key );
        });
        Console.WriteLine("-----------------------");
    }

    static void addTtl(string key, int ttl) 
    {
        var next = Now() + ttl;
        nodes.Add(new TtlNode { Key = key, Utime = next });
        minify();
        DebugPrint();
        if (next <= nodes[0].Utime) {
            TtlJob(key, next);
            running++;
        }
    }

    static void TtlJob(string key, long next) {
        Task.Run(async () =>
        {
            var now = Now();
            if (next > now)
                await Task.Delay(TimeSpan.FromSeconds(next - now), CancellationToken.None);

            lock (_lock)
            {
                if (nodes.Count > 0)
                {
                    nodes[0] = nodes[nodes.Count -1];
                    nodes.RemoveAt(nodes.Count -1);
                    heapify(0);
                    dict.Remove(key);
                    Console.WriteLine("Ran Job for " + key);
                    DebugPrint();
                }
                if (--running == 0) {
                    running++;
                    TtlJob(nodes[0].Key, nodes[0].Utime);
                }
            }
        });
    }

    static long Now() { return DateTimeOffset.UtcNow.ToUnixTimeSeconds(); }

    static int up(int x) { return x / 2;}
    static int left(int x) { return x * 2;}
    static int right(int x) { return (x * 2) + 1; }

    static void minify()
    {
        var i = nodes.Count - 1;
        do {
            var u = up(i);
            if (i <= 0 || nodes[u].Utime < nodes[i].Utime) break;
            swap(i, u);
            i = u;
        } while(true);
    }

    static void swap(int x, int y)
    {
        var z = nodes[x];
        nodes[x] = nodes[y];
        nodes[y] = z;
    }

    static void heapify(int i)
    {
        int s = i;
        int l = left(i);
        int r = right(i);
        if (l < nodes.Count && nodes[l].Utime < nodes[s].Utime)
            s = l;
        if (r < nodes.Count && nodes[r].Utime < nodes[s].Utime)
            s = r;
        if (s != i) {
            swap(s, i);
            heapify(s);
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using StackExchange.Redis;

namespace Lab.Redis.Clustering.Application
{
    public class Program
    {
        private static void Main()
        {
            const string redisConnectionString = "cluster1.redis.cache.windows.net:6379, password=9eshMbP0t9J7TFHYnGX8jhD87zlkBeJsv8qh8CvLVqM=";
            var options = ConfigurationOptions.Parse(redisConnectionString); 
            Console.WriteLine("App Start...");
            
            using (var conn = ConnectionMultiplexer.Connect(options))
            {
                var db = conn.GetDatabase();
                Console.WriteLine($"Current Value:{db.StringGet("test").ToString()}");

                var loadedLuaScripts = new Dictionary<LuaScriptEnum, LoadedLuaScript>
                {
                    {
                        LuaScriptEnum.AddValueWithTargetKey, PrepareLuaScript(conn.GetServer(options.EndPoints.First()),
                            @"local targetKey = KEYS[1] -- target key for redis
local initValue =  tonumber(ARGV[1]) -- initial value for target key if not exist and need to create
local incrementValue =  tonumber(ARGV[2]) -- increment value for target key when exist and calling for each time

local currentValue =  initValue 

-- create key with 0 if key is not exist 
local isNewKey = redis.call('SETNX',targetKey,currentValue)

-- add current value if key is exist
if isNewKey == initValue then 

  currentValue = redis.call('GET',targetKey)

  if currentValue then

    currentValue = currentValue + incrementValue

  end

  redis.call('SET',targetKey, currentValue)

end 

-- return key value
return currentValue")
                    }
                };

                var executedReturn = db.ScriptEvaluate(loadedLuaScripts[LuaScriptEnum.AddValueWithTargetKey].Hash,
                    new RedisKey[] {"test"},
                    new RedisValue[] {0, 50});
                Console.WriteLine(executedReturn);
            }
        }
        
        private static LoadedLuaScript PrepareLuaScript(IServer server, string luaScriptContent)
        {
            return LuaScript
                .Prepare(luaScriptContent)
                .Load(server,CommandFlags.DemandMaster);
        }
    }

    public enum LuaScriptEnum
    {
        AddValueWithTargetKey
    }
    
}
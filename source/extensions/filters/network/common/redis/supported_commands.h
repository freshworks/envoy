#pragma once

#include <set>
#include <string>
#include <vector>

#include "source/common/common/macros.h"

#include "absl/container/flat_hash_set.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace Common {
namespace Redis {

struct SupportedCommands {
  /**
   * @return commands which hash to a single server
   */
  static const absl::flat_hash_set<std::string>& simpleCommands() {
    CONSTRUCT_ON_FIRST_USE(
        absl::flat_hash_set<std::string>, "append", "bitcount", "bitfield", "bitpos", "decr",
        "decrby", "dump", "expire", "expireat", "geoadd", "geodist", "geohash", "geopos",
        "georadius_ro", "georadiusbymember_ro", "get", "getbit", "getdel", "getrange", "getset",
        "hdel", "hexists", "hget", "hgetall", "hincrby", "hincrbyfloat", "hkeys", "hlen", "hmget",
        "hmset", "hscan", "hset", "hsetnx", "hstrlen", "hvals", "incr", "incrby", "incrbyfloat",
        "lindex", "linsert", "llen", "lmove", "lpop", "lpush", "lpushx", "lrange", "lrem", "lset",
        "ltrim", "persist", "pexpire", "pexpireat", "pfadd", "pfcount", "psetex", "pttl", "restore",
        "rpop", "rpush", "rpushx", "sadd", "scard", "set", "setbit", "setex", "setnx", "setrange",
        "sismember", "smembers", "spop", "srandmember", "srem", "sscan", "strlen", "ttl", "type",
        "watch", "zadd", "zcard", "zcount", "zincrby", "zlexcount", "zpopmin", "zpopmax", "zrange",
        "zrangebylex", "zrangebyscore", "zrank", "zrem", "zremrangebylex", "zremrangebyrank",
        "zremrangebyscore", "zrevrange", "zrevrangebylex", "zrevrangebyscore", "zrevrank", "zscan",
        "zscore", "rpoplpush", "smove", "sunion", "sdiff", "sinter", "sinterstore", "zunionstore", 
        "zinterstore", "pfmerge", "georadius", "georadiusbymember", "xadd", "xlen", "xdel", "xtrim", 
        "xrange", "xrevrange", "rename", "getex", "sort", "zmscore", "sdiffstore", "msetnx", "substr",
        "zrangestore", "zunion", "echo", "zdiff", "xautoclaim", "xinfo", "sunionstore", "smismember",
        "hrandfield", "geosearchstore", "zdiffstore", "geosearch", "randomkey", "zinter", "zrandmember",
        "bitop", "xclaim", "lpos", "renamenx", "xgroup","xreadnonblock");
  }

  /**
   * @return commands which hash on the fourth argument
   */
  static const absl::flat_hash_set<std::string>& evalCommands() {
    CONSTRUCT_ON_FIRST_USE(absl::flat_hash_set<std::string>, "eval", "evalsha");
  }

  /**
   * @return commands which are sent to multiple servers and coalesced by summing the responses
   */
  static const absl::flat_hash_set<std::string>& hashMultipleSumResultCommands() {
    CONSTRUCT_ON_FIRST_USE(absl::flat_hash_set<std::string>, "del", "exists", "touch", "unlink");
  }

  /**
   * @return commands which handle Redis transactions.
   */
  static const absl::flat_hash_set<std::string>& transactionCommands() {
    CONSTRUCT_ON_FIRST_USE(absl::flat_hash_set<std::string>, "multi", "exec", "discard");
  }

  /**
   * @return commands which handle Redis transactions allowed non simple commands.
   */
  static const absl::flat_hash_set<std::string>& transactionAllowedNonSimpleCommands() {
    CONSTRUCT_ON_FIRST_USE(absl::flat_hash_set<std::string>, "del", "exists", "touch", "unlink");
  }

   /**
   * @return commands which handle Redis blocking operations.
   */
  static const absl::flat_hash_set<std::string>& subscriptionCommands() {
    CONSTRUCT_ON_FIRST_USE(absl::flat_hash_set<std::string>, "subscribe", "psubscribe", "unsubscribe", "punsubscribe");
  }

 /**
   * @return commands allowed when a client is in subscribed state.
   */
  static const absl::flat_hash_set<std::string>& subcrStateallowedCommands() {
    CONSTRUCT_ON_FIRST_USE(absl::flat_hash_set<std::string>, "subscribe", "psubscribe", "unsubscribe", "punsubscribe","quit", "ping","reset");
  }

  /**
   * @return commands that make a client to enter subscribed state.
   */
  static const absl::flat_hash_set<std::string>& subcrStateEnterCommands() {
    CONSTRUCT_ON_FIRST_USE(absl::flat_hash_set<std::string>, "subscribe", "psubscribe");
  }

   /**
   * @return commands that are called blocking commands but not pubsub commands.
   */
  static const absl::flat_hash_set<std::string>& blockingCommands() {
    CONSTRUCT_ON_FIRST_USE(absl::flat_hash_set<std::string>, "blpop", "brpop", "brpoplpush", "bzpopmax", "bzpopmin", "xreadblock", "xreadgroup", "blmove");
  }

  /**
   * @return commands that do not require arguments.
   */
  static const absl::flat_hash_set<std::string>& noArgCommands() {
    CONSTRUCT_ON_FIRST_USE(absl::flat_hash_set<std::string>, "info", "flushall", "flushdb", "unwatch");
  }
  
    /**
   * @return commands which handle Redis commands without keys.
   */
  static const absl::flat_hash_set<std::string>& adminNokeyCommands() {
    CONSTRUCT_ON_FIRST_USE(absl::flat_hash_set<std::string>, "script", "flushall", "flushdb", "publish","pubsub", "keys", "slowlog", "config","client","info","cluster","select", "unwatch");
  }

  /**
  * @return client sub commands thats supported 
  */
  static const absl::flat_hash_set<std::string>& clientSubCommands() {
    CONSTRUCT_ON_FIRST_USE(absl::flat_hash_set<std::string>, "getname", "list","setname");
  }

    /**
   * @return commands which handle Redis commands without keys.
   */
  static const absl::flat_hash_set<std::string>& allShardCommands() {
    CONSTRUCT_ON_FIRST_USE(absl::flat_hash_set<std::string>, "script", "flushall", "flushdb", "pubsub", "keys", "slowlog", "config", "client", "info", "select", "unwatch");
    }
  
  /**
   * @return scan command
   */
  static const std::string& scan() { CONSTRUCT_ON_FIRST_USE(std::string, "scan"); }

  /**
   * @return auth command
   */
  static const std::string& auth() { CONSTRUCT_ON_FIRST_USE(std::string, "auth"); }

  /**
   * @return hello command
   */
  static const std::string& hello() { CONSTRUCT_ON_FIRST_USE(std::string, "hello"); }

  /**
   * @return mget command
   */
  static const std::string& mget() { CONSTRUCT_ON_FIRST_USE(std::string, "mget"); }

  /**
   * @return mset command
   */
  static const std::string& mset() { CONSTRUCT_ON_FIRST_USE(std::string, "mset"); }

  /**
   * @return ping command
   */
  static const std::string& ping() { CONSTRUCT_ON_FIRST_USE(std::string, "ping"); }

  /**
   * @return time command
   */
  static const std::string& time() { CONSTRUCT_ON_FIRST_USE(std::string, "time"); }

  /**
   * @return quit command
   */
  static const std::string& quit() { CONSTRUCT_ON_FIRST_USE(std::string, "quit"); }

  /**
   * @return quit command
   */
  static const std::string& exit() { CONSTRUCT_ON_FIRST_USE(std::string, "exit"); }

    /**
   * @return info command
   */
  static const std::string& info() { CONSTRUCT_ON_FIRST_USE(std::string, "info"); }

    /**
   * @return special stream commands
   */
  static const std::string& spl_strm_commands() { CONSTRUCT_ON_FIRST_USE(std::string, "xread"); }
  /**
   * @return commands which alters the state of redis
   */
  static const absl::flat_hash_set<std::string>& writeCommands() {
    CONSTRUCT_ON_FIRST_USE(absl::flat_hash_set<std::string>, "append", "bitfield", "decr", "decrby",
                           "del", "discard", "exec", "expire", "expireat", "eval", "evalsha",
                           "geoadd", "getdel", "hdel", "hincrby", "hincrbyfloat", "hmset", "hset",
                           "hsetnx", "incr", "incrby", "incrbyfloat", "linsert", "lmove", "lpop",
                           "lpush", "lpushx", "lrem", "lset", "ltrim", "mset", "multi", "persist",
                           "pexpire", "pexpireat", "pfadd", "psetex", "restore", "rpop", "rpush",
                           "rpushx", "sadd", "set", "setbit", "setex", "setnx", "setrange", "spop",
                           "srem", "zadd", "zincrby", "touch", "zpopmin", "zpopmax", "zrem",
                           "zremrangebylex", "zremrangebyrank", "zremrangebyscore", "unlink", "rpoplpush", 
                           "smove", "sinterstore", "zunionstore", "zinterstore", "pfmerge", "xgroup",
                           "georadius", "georadiusbymember", "xadd", "xtrim", "xdel", "rename", "getex",
                           "sort", "sdiffstore", "msetnx", "zrangestore", "zunion", "zdiff", "xautoclaim",
                           "sunionstore", "geosearchstore", "zdiffstore", "bitop", "xclaim", "renamenx");
  }

  static bool isReadCommand(const std::string& command) {
    return !writeCommands().contains(command);
  }
};

} // namespace Redis
} // namespace Common
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

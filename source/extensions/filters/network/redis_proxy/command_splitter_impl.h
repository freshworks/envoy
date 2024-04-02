#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "envoy/stats/scope.h"
#include "envoy/stats/stats_macros.h"
#include "envoy/stats/timespan.h"

#include "source/common/common/logger.h"
#include "source/common/common/utility.h"
#include "source/common/stats/timespan_impl.h"
#include "source/extensions/filters/network/common/redis/client_impl.h"
#include "source/extensions/filters/network/common/redis/fault_impl.h"
#include "source/extensions/filters/network/common/redis/utility.h"
#include "source/extensions/filters/network/redis_proxy/command_splitter.h"
#include "source/extensions/filters/network/redis_proxy/conn_pool_impl.h"
#include "source/extensions/filters/network/redis_proxy/router.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace RedisProxy {
namespace CommandSplitter {

struct ResponseValues {
  const std::string OK = "OK";
  const std::string InvalidRequest = "invalid request";
  const std::string NoUpstreamHost = "no upstream host";
  const std::string UpstreamFailure = "upstream failure";
  const std::string UpstreamProtocolError = "upstream protocol error";
  const std::string AuthRequiredError = "NOAUTH Authentication required.";
  const std::string UnKnownCommandHello = "ERR unknown command 'HELLO'";
};

using Response = ConstSingleton<ResponseValues>;

/**
 * All command level stats. @see stats_macros.h
 */
#define ALL_COMMAND_STATS(COUNTER)                                                                 \
  COUNTER(total)                                                                                   \
  COUNTER(success)                                                                                 \
  COUNTER(error)                                                                                   \
  COUNTER(error_fault)                                                                             \
  COUNTER(delay_fault)

enum class AdminRespHandlerType{
  singleshardresponse,
  allresponses_mustbe_same,
  aggregate_all_responses,
  iteraterequest_on_response,
  response_handler_none
};
/**
 * Struct definition for all command stats. @see stats_macros.h
 */
struct CommandStats {
  ALL_COMMAND_STATS(GENERATE_COUNTER_STRUCT)
  Envoy::Stats::Histogram& latency_;
};

class CommandHandler {
public:
  virtual ~CommandHandler() = default;

  virtual SplitRequestPtr startRequest(Common::Redis::RespValuePtr&& request,
                                       SplitCallbacks& callbacks, CommandStats& command_stats,
                                       TimeSource& time_source, bool delay_command_latency,
                                       const StreamInfo::StreamInfo& stream_info) PURE;
};

class CommandHandlerBase {
protected:
  CommandHandlerBase(Router& router) : router_(router) {}

  Router& router_;
};

class SplitRequestBase : public SplitRequest, public Logger::Loggable<Logger::Id::redis> {
protected:
  static void onWrongNumberOfArguments(SplitCallbacks& callbacks,
                                       const Common::Redis::RespValue& request);
  void updateStats(const bool success);

  SplitRequestBase(CommandStats& command_stats, TimeSource& time_source, bool delay_command_latency)
      : command_stats_(command_stats) {
    if (!delay_command_latency) {
      command_latency_ = std::make_unique<Stats::HistogramCompletableTimespanImpl>(
          command_stats_.latency_, time_source);
    } else {
      command_latency_ = nullptr;
    }
  }
  CommandStats& command_stats_;
  Stats::TimespanPtr command_latency_;
};

/**
 * AdministrationRequest is a base class for all admin commands or commands which does not work on keys.
 */
class AdministrationRequest : public SplitRequestBase {
public:
  ~AdministrationRequest() override;

  // RedisProxy::CommandSplitter::SplitRequest
  void cancel() override;

protected:
  AdministrationRequest(SplitCallbacks& callbacks, CommandStats& command_stats,
                      TimeSource& time_source, bool delay_command_latency)
      : SplitRequestBase(command_stats, time_source, delay_command_latency), callbacks_(callbacks) {
  }
  struct PendingRequest : public ConnPool::PoolCallbacks {
    PendingRequest(AdministrationRequest& parent, uint32_t reqindex,int32_t shardindex, std::string rediscommand, std::string redisarg, AdminRespHandlerType resphandletype) : parent_(parent), reqindex_(reqindex),shard_index_(shardindex),rediscommand_(rediscommand),redisarg_(redisarg),admin_resp_handler_type_(resphandletype) {}

    // ConnPool::PoolCallbacks
    void onResponse(Common::Redis::RespValuePtr&& value) override {
      if(admin_resp_handler_type_ == AdminRespHandlerType::singleshardresponse){
        parent_.onSingleShardresponse(std::move(value), reqindex_,shard_index_);
      }else if(admin_resp_handler_type_ == AdminRespHandlerType::allresponses_mustbe_same){
        parent_.onAllChildResponseSame(std::move(value), reqindex_,shard_index_);
      }else if(admin_resp_handler_type_ == AdminRespHandlerType::aggregate_all_responses){
        parent_.onallChildRespAgrregate(std::move(value), reqindex_,shard_index_);      
      }else{
        ASSERT(false);
      }
    }
    void onFailure() override {
      if(admin_resp_handler_type_ == AdminRespHandlerType::singleshardresponse){
        parent_.onSingleShardResponseFailure(reqindex_,shard_index_);
      }else if(admin_resp_handler_type_ == AdminRespHandlerType::allresponses_mustbe_same){
        parent_.onallChildResponseSameFailure(reqindex_,shard_index_);
      }else if(admin_resp_handler_type_ == AdminRespHandlerType::aggregate_all_responses){
        parent_.onallChildRespAgrregateFail(reqindex_,shard_index_);      
      }else{
        ASSERT(false);
      }
    }

    AdministrationRequest& parent_;
    const int32_t reqindex_;
    int32_t shard_index_=-1;
    std::string rediscommand_;
    std::string redisarg_;
    Common::Redis::Client::PoolRequest* handle_{};
    AdminRespHandlerType admin_resp_handler_type_ = AdminRespHandlerType::response_handler_none;
  };
  virtual void onSingleShardresponse(Common::Redis::RespValuePtr&& value, int32_t reqindex,int32_t shardindex) PURE;
  virtual void onAllChildResponseSame(Common::Redis::RespValuePtr&& value, int32_t reqindex,int32_t shardindex) PURE;
  virtual void onallChildRespAgrregate(Common::Redis::RespValuePtr&& value, int32_t reqindex,int32_t shardindex) PURE;
  void onSingleShardResponseFailure(int32_t reqindex, int32_t shardindex);
  void onallChildResponseSameFailure(int32_t reqindex,int32_t shardindex);
  void onallChildRespAgrregateFail(int32_t reqindex,int32_t shardindex);

  SplitCallbacks& callbacks_;
  std::vector<Common::Redis::RespValuePtr> pending_responses_;
  std::vector<PendingRequest> pending_requests_;
  u_int32_t singleShardPendingRequestIndex_;
  int32_t num_pending_responses_;
  uint32_t error_count_{0};
  int32_t response_index_{0};
  int32_t num_of_Shards_{0};

};

// TODO: The base class and scan request class should be removed during the optimization
class ScanRequestBase: public SplitRequestBase {
public:
  ~ScanRequestBase() override;

  // RedisProxy::CommandSplitter::SplitRequest
  void cancel() override;

protected:
  ScanRequestBase(SplitCallbacks& callbacks, CommandStats& command_stats,
                      TimeSource& time_source, bool delay_command_latency)
      : SplitRequestBase(command_stats, time_source, delay_command_latency), callbacks_(callbacks) {
  }
  struct PendingRequest : public ConnPool::PoolCallbacks {
    PendingRequest(ScanRequestBase& parent, uint32_t index, int32_t shardindex) : parent_(parent), index_(index), shard_index_(shardindex) {}

    // ConnPool::PoolCallbacks
    void onResponse(Common::Redis::RespValuePtr&& value) override {
      parent_.onChildResponse(std::move(value), index_, shard_index_);
    }
    void onFailure() override { parent_.onChildFailure(index_, shard_index_); }

    ScanRequestBase& parent_;
    const int32_t index_;
    int32_t count_;
    int32_t shard_index_=-1;
    Common::Redis::Client::PoolRequest* handle_{};
  };

  virtual void onChildResponse(Common::Redis::RespValuePtr&& value, int32_t index, int32_t shardindex) PURE;
  void onChildFailure(int32_t index, int32_t shardindex);

  SplitCallbacks& callbacks_;
  RouteSharedPtr route_;

  std::vector<Common::Redis::RespValuePtr> pending_responses_;
  std::vector<PendingRequest> pending_requests_;
  Common::Redis::RespValue request_;
  int32_t num_pending_responses_{0};
  uint32_t error_count_{0};
  std::string resp_obj_count_;
  int32_t response_index_{0};
  int32_t num_of_Shards_{0};
  
};

/**
 * SingleServerRequest is a base class for commands that hash to a single backend.
 */
class SingleServerRequest : public SplitRequestBase, public ConnPool::PoolCallbacks {
public:
  ~SingleServerRequest() override;

  // ConnPool::PoolCallbacks
  void onResponse(Common::Redis::RespValuePtr&& response) override;
  void onFailure() override;
  void onFailure(std::string error_msg);

  // RedisProxy::CommandSplitter::SplitRequest
  void cancel() override;

protected:
  SingleServerRequest(SplitCallbacks& callbacks, CommandStats& command_stats,
                      TimeSource& time_source, bool delay_command_latency)
      : SplitRequestBase(command_stats, time_source, delay_command_latency), callbacks_(callbacks) {
  }

  SplitCallbacks& callbacks_;
  ConnPool::InstanceSharedPtr conn_pool_;
  Common::Redis::Client::PoolRequest* handle_{};
  Common::Redis::RespValuePtr incoming_request_;
};

/**
 * ErrorFaultRequest returns an error.
 */
class ErrorFaultRequest : public SingleServerRequest {
public:
  static SplitRequestPtr create(SplitCallbacks& callbacks, CommandStats& command_stats,
                                TimeSource& time_source, bool has_delaydelay_command_latency_fault,
                                const StreamInfo::StreamInfo& stream_info);

private:
  ErrorFaultRequest(SplitCallbacks& callbacks, CommandStats& command_stats, TimeSource& time_source,
                    bool delay_command_latency)
      : SingleServerRequest(callbacks, command_stats, time_source, delay_command_latency) {}
};

/**
 * DelayFaultRequest wraps a request- either a normal request or a fault- and delays it.
 */
class DelayFaultRequest : public SplitRequestBase, public SplitCallbacks {
public:
  static std::unique_ptr<DelayFaultRequest>
  create(SplitCallbacks& callbacks, CommandStats& command_stats, TimeSource& time_source,
         Event::Dispatcher& dispatcher, std::chrono::milliseconds delay,
         const StreamInfo::StreamInfo& stream_info);

  DelayFaultRequest(SplitCallbacks& callbacks, CommandStats& command_stats, TimeSource& time_source,
                    Event::Dispatcher& dispatcher, std::chrono::milliseconds delay)
      : SplitRequestBase(command_stats, time_source, false), callbacks_(callbacks), delay_(delay) {
    delay_timer_ = dispatcher.createTimer([this]() -> void { onDelayResponse(); });
  }

  // SplitCallbacks
  bool connectionAllowed() override { return callbacks_.connectionAllowed(); }
  void onQuit() override { callbacks_.onQuit(); }
  void onAuth(const std::string& password) override { callbacks_.onAuth(password); }
  void onAuth(const std::string& username, const std::string& password) override {
    callbacks_.onAuth(username, password);
  }
  void setClientname(std::string clientname) override { callbacks_.setClientname(clientname); }
  std::string getClientname() override { return callbacks_.getClientname(); }

  void onResponse(Common::Redis::RespValuePtr&& response) override;
  Common::Redis::Client::Transaction& transaction() override { return callbacks_.transaction(); }

  // RedisProxy::CommandSplitter::SplitRequest
  void cancel() override;

  SplitRequestPtr wrapped_request_ptr_;

private:
  void onDelayResponse();

  SplitCallbacks& callbacks_;
  std::chrono::milliseconds delay_;
  Event::TimerPtr delay_timer_;
  Common::Redis::RespValuePtr response_;
};

class mgmtNoKeyRequest : public AdministrationRequest {
public:
  static SplitRequestPtr create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                SplitCallbacks& callbacks, CommandStats& command_stats,
                                TimeSource& time_source, bool delay_command_latency,
                                const StreamInfo::StreamInfo& stream_info);

private:
  mgmtNoKeyRequest(SplitCallbacks& callbacks, CommandStats& command_stats, TimeSource& time_source,
                bool delay_command_latency)
      : AdministrationRequest(callbacks, command_stats, time_source, delay_command_latency) {}

  // RedisProxy::CommandSplitter::AdministrationRequest
  void onSingleShardresponse(Common::Redis::RespValuePtr&& value, int32_t reqindex,int32_t shardindex) override;
  void onAllChildResponseSame(Common::Redis::RespValuePtr&& value, int32_t reqindex,int32_t shardindex) override;
  void onallChildRespAgrregate(Common::Redis::RespValuePtr&& value, int32_t reqindex,int32_t shardindex) override;

};

class PubSubRequest : public AdministrationRequest {
public:
  static SplitRequestPtr create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                SplitCallbacks& callbacks, CommandStats& command_stats,
                                TimeSource& time_source, bool delay_command_latency,
                                const StreamInfo::StreamInfo& stream_info);
private:
  PubSubRequest(SplitCallbacks& callbacks, CommandStats& command_stats, TimeSource& time_source,
                bool delay_command_latency)
      : AdministrationRequest(callbacks, command_stats, time_source, delay_command_latency) {}
  
  void onSingleShardresponse(Common::Redis::RespValuePtr&& value, int32_t reqindex,int32_t shardindex) override;
  void onAllChildResponseSame(Common::Redis::RespValuePtr&& value, int32_t reqindex,int32_t shardindex) override;
  void onallChildRespAgrregate(Common::Redis::RespValuePtr&& value, int32_t reqindex,int32_t shardindex) override;
  //Common::Redis::Client::PoolRequest* handle_{};
};

class BlockingClientRequest : public SingleServerRequest {
public:
  static SplitRequestPtr create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                SplitCallbacks& callbacks, CommandStats& command_stats,
                                TimeSource& time_source, bool delay_command_latency,
                                const StreamInfo::StreamInfo& stream_info);
private:
  BlockingClientRequest(SplitCallbacks& callbacks, CommandStats& command_stats, TimeSource& time_source,
                bool delay_command_latency)
      : SingleServerRequest(callbacks, command_stats, time_source, delay_command_latency) {}

};

/**
 * SimpleRequest hashes the first argument as the key.
 */
class SimpleRequest : public SingleServerRequest {
public:
  static SplitRequestPtr create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                SplitCallbacks& callbacks, CommandStats& command_stats,
                                TimeSource& time_source, bool delay_command_latency,
                                const StreamInfo::StreamInfo& stream_info);

private:
  SimpleRequest(SplitCallbacks& callbacks, CommandStats& command_stats, TimeSource& time_source,
                bool delay_command_latency)
      : SingleServerRequest(callbacks, command_stats, time_source, delay_command_latency) {}
};

/**
 * EvalRequest hashes the fourth argument as the key.
 */
class EvalRequest : public SingleServerRequest {
public:
  static SplitRequestPtr create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                SplitCallbacks& callbacks, CommandStats& command_stats,
                                TimeSource& time_source, bool delay_command_latency,
                                const StreamInfo::StreamInfo& stream_info);

private:
  EvalRequest(SplitCallbacks& callbacks, CommandStats& command_stats, TimeSource& time_source,
              bool delay_command_latency)
      : SingleServerRequest(callbacks, command_stats, time_source, delay_command_latency) {}
};

/**
 * TransactionRequest handles commands that are part of a Redis transaction.
 * This includes MULTI, EXEC, DISCARD, and also all the commands that are
 * part of the transaction.
 */
class TransactionRequest : public SingleServerRequest {
public:
  static SplitRequestPtr create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                SplitCallbacks& callbacks, CommandStats& command_stats,
                                TimeSource& time_source, bool delay_command_latency,
                                const StreamInfo::StreamInfo& stream_info);

private:
  TransactionRequest(SplitCallbacks& callbacks, CommandStats& command_stats,
                     TimeSource& time_source, bool delay_command_latency)
      : SingleServerRequest(callbacks, command_stats, time_source, delay_command_latency) {}
};

/**
 * ScanRequest handles scanning the database of all shards sequentially.
 */

class ScanRequest : public ScanRequestBase {
public:
  static SplitRequestPtr create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                SplitCallbacks& callbacks, CommandStats& command_stats,
                                TimeSource& time_source, bool delay_command_latency,
                                const StreamInfo::StreamInfo& stream_info);

private:
  ScanRequest(SplitCallbacks& callbacks, CommandStats& command_stats, TimeSource& time_source,
                bool delay_command_latency)
      : ScanRequestBase(callbacks, command_stats, time_source, delay_command_latency) {}

  void onChildResponse(Common::Redis::RespValuePtr&& value, int32_t index, int32_t shardindex) override;
  void onChildError(Common::Redis::RespValuePtr&& value);
};

/**
 * FragmentedRequest is a base class for requests that contains multiple keys. An individual request
 * is sent to the appropriate server for each key. The responses from all servers are combined and
 * returned to the client.
 */
class FragmentedRequest : public SplitRequestBase {
public:
  ~FragmentedRequest() override;

  // RedisProxy::CommandSplitter::SplitRequest
  void cancel() override;

protected:
  FragmentedRequest(SplitCallbacks& callbacks, CommandStats& command_stats, TimeSource& time_source,
                    bool delay_command_latency)
      : SplitRequestBase(command_stats, time_source, delay_command_latency), callbacks_(callbacks) {
  }

  struct PendingRequest : public ConnPool::PoolCallbacks {
    PendingRequest(FragmentedRequest& parent, uint32_t index) : parent_(parent), index_(index) {}

    // ConnPool::PoolCallbacks
    void onResponse(Common::Redis::RespValuePtr&& value) override {
      parent_.onChildResponse(std::move(value), index_);
    }
    void onFailure() override { parent_.onChildFailure(index_); }

    FragmentedRequest& parent_;
    const uint32_t index_;
    Common::Redis::Client::PoolRequest* handle_{};
  };

  virtual void onChildResponse(Common::Redis::RespValuePtr&& value, uint32_t index) PURE;
  void onChildFailure(uint32_t index);

  SplitCallbacks& callbacks_;

  Common::Redis::RespValuePtr pending_response_;
  std::vector<PendingRequest> pending_requests_;
  uint32_t num_pending_responses_;
  uint32_t error_count_{0};
};

/**
 * MGETRequest takes each key from the command and sends a GET for each to the appropriate Redis
 * server. The response contains the result from each command.
 */
class MGETRequest : public FragmentedRequest {
public:
  static SplitRequestPtr create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                SplitCallbacks& callbacks, CommandStats& command_stats,
                                TimeSource& time_source, bool delay_command_latency,
                                const StreamInfo::StreamInfo& stream_info);

private:
  MGETRequest(SplitCallbacks& callbacks, CommandStats& command_stats, TimeSource& time_source,
              bool delay_command_latency)
      : FragmentedRequest(callbacks, command_stats, time_source, delay_command_latency) {}

  // RedisProxy::CommandSplitter::FragmentedRequest
  void onChildResponse(Common::Redis::RespValuePtr&& value, uint32_t index) override;
};

/**
 * SplitKeysSumResultRequest takes each key from the command and sends the same incoming command
 * with each key to the appropriate Redis server. The response from each Redis (which must be an
 * integer) is summed and returned to the user. If there is any error or failure in processing the
 * fragmented commands, an error will be returned.
 */
class SplitKeysSumResultRequest : public FragmentedRequest {
public:
  static SplitRequestPtr create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                SplitCallbacks& callbacks, CommandStats& command_stats,
                                TimeSource& time_source, bool delay_command_latency,
                                const StreamInfo::StreamInfo& stream_info);

private:
  SplitKeysSumResultRequest(SplitCallbacks& callbacks, CommandStats& command_stats,
                            TimeSource& time_source, bool delay_command_latency)
      : FragmentedRequest(callbacks, command_stats, time_source, delay_command_latency) {}

  // RedisProxy::CommandSplitter::FragmentedRequest
  void onChildResponse(Common::Redis::RespValuePtr&& value, uint32_t index) override;

  int64_t total_{0};
};

/**
 * MSETRequest takes each key and value pair from the command and sends a SET for each to the
 * appropriate Redis server. The response is an OK if all commands succeeded or an ERR if any
 * failed.
 */
class MSETRequest : public FragmentedRequest {
public:
  static SplitRequestPtr create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                SplitCallbacks& callbacks, CommandStats& command_stats,
                                TimeSource& time_source, bool delay_command_latency,
                                const StreamInfo::StreamInfo& stream_info);

private:
  MSETRequest(SplitCallbacks& callbacks, CommandStats& command_stats, TimeSource& time_source,
              bool delay_command_latency)
      : FragmentedRequest(callbacks, command_stats, time_source, delay_command_latency) {}

  // RedisProxy::CommandSplitter::FragmentedRequest
  void onChildResponse(Common::Redis::RespValuePtr&& value, uint32_t index) override;
};

/**
 * CommandHandlerFactory is placed in the command lookup map for each supported command and is used
 * to create Request objects.
 */
template <class RequestClass>
class CommandHandlerFactory : public CommandHandler, CommandHandlerBase {
public:
  CommandHandlerFactory(Router& router) : CommandHandlerBase(router) {}
  SplitRequestPtr startRequest(Common::Redis::RespValuePtr&& request, SplitCallbacks& callbacks,
                               CommandStats& command_stats, TimeSource& time_source,
                               bool delay_command_latency,
                               const StreamInfo::StreamInfo& stream_info) override {
    return RequestClass::create(router_, std::move(request), callbacks, command_stats, time_source,
                                delay_command_latency, stream_info);
  }
};

/**
 * All splitter stats. @see stats_macros.h
 */
#define ALL_COMMAND_SPLITTER_STATS(COUNTER)                                                        \
  COUNTER(invalid_request)                                                                         \
  COUNTER(unsupported_command)                                                                     \
  COUNTER(auth_failure)
/**
 * Struct definition for all splitter stats. @see stats_macros.h
 */
struct InstanceStats {
  ALL_COMMAND_SPLITTER_STATS(GENERATE_COUNTER_STRUCT)
};

class InstanceImpl : public Instance, Logger::Loggable<Logger::Id::redis> {
public:
  InstanceImpl(RouterPtr&& router, Stats::Scope& scope, const std::string& stat_prefix,
               TimeSource& time_source, bool latency_in_micros,
               Common::Redis::FaultManagerPtr&& fault_manager);

  // RedisProxy::CommandSplitter::Instance
  SplitRequestPtr makeRequest(Common::Redis::RespValuePtr&& request, SplitCallbacks& callbacks,
                              Event::Dispatcher& dispatcher,
                              const StreamInfo::StreamInfo& stream_info) override;

private:
  friend class RedisCommandSplitterImplTest;

  struct HandlerData {
    CommandStats command_stats_;
    std::reference_wrapper<CommandHandler> handler_;
  };

  using HandlerDataPtr = std::shared_ptr<HandlerData>;

  void addHandler(Stats::Scope& scope, const std::string& stat_prefix, const std::string& name,
                  bool latency_in_micros, CommandHandler& handler);
  void onInvalidRequest(SplitCallbacks& callbacks);

  RouterPtr router_;
  CommandHandlerFactory<SimpleRequest> simple_command_handler_;
  CommandHandlerFactory<EvalRequest> eval_command_handler_;
  CommandHandlerFactory<MGETRequest> mget_handler_;
  CommandHandlerFactory<MSETRequest> mset_handler_;
  CommandHandlerFactory<SplitKeysSumResultRequest> split_keys_sum_result_handler_;
  CommandHandlerFactory<TransactionRequest> transaction_handler_;
  CommandHandlerFactory<mgmtNoKeyRequest> mgmt_nokey_request_handler_;
  CommandHandlerFactory<ScanRequest> scanrequest_handler_;
  CommandHandlerFactory<PubSubRequest> subscription_handler_;
  CommandHandlerFactory<BlockingClientRequest> blocking_client_request_handler_;
  TrieLookupTable<HandlerDataPtr> handler_lookup_table_;
  InstanceStats stats_;
  TimeSource& time_source_;
  Common::Redis::FaultManagerPtr fault_manager_;
};

} // namespace CommandSplitter
} // namespace RedisProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

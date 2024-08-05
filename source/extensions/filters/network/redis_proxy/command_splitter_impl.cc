#include "source/extensions/filters/network/redis_proxy/command_splitter_impl.h"

#include "source/common/common/logger.h"
#include "source/extensions/filters/network/common/redis/supported_commands.h"
#include <string>

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace RedisProxy {
namespace CommandSplitter {
namespace {

// null_pool_callbacks is used for requests that must be filtered and not redirected such as
// "asking".
ConnPool::DoNothingPoolCallbacks null_pool_callbacks;

/**
 * Make request and maybe mirror the request based on the mirror policies of the route.
 * @param route supplies the route matched with the request.
 * @param command supplies the command of the request.
 * @param key supplies the key of the request.
 * @param incoming_request supplies the request.
 * @param callbacks supplies the request completion callbacks.
 * @param transaction supplies the transaction info of the current connection.
 * @return PoolRequest* a handle to the active request or nullptr if the request could not be made
 *         for some reason.
 */
Common::Redis::Client::PoolRequest* makeSingleServerRequest(
    const RouteSharedPtr& route, const std::string& command, const std::string& key,
    Common::Redis::RespValueConstSharedPtr incoming_request, ConnPool::PoolCallbacks& callbacks,
    Common::Redis::Client::Transaction& transaction) {
  // If a transaction is active, clients_[0] is the primary connection to the cluster.
  // The subsequent clients in the array are used for mirroring.
  transaction.current_client_idx_ = 0;
  auto handler = route->upstream(command)->makeRequest(key, ConnPool::RespVariant(incoming_request),
                                                       callbacks, transaction);
  if (handler) {
    for (auto& mirror_policy : route->mirrorPolicies()) {
      transaction.current_client_idx_++;
      if (mirror_policy->shouldMirror(command)) {
        mirror_policy->upstream()->makeRequest(key, ConnPool::RespVariant(incoming_request),
                                               null_pool_callbacks, transaction);
      }
    }
  }
  return handler;
}
AdminRespHandlerType getresponseHandlerType(const std::string& command_name) {
    // Define a map to store the mappings between command names and response handler types
    static const std::unordered_map<std::string, AdminRespHandlerType> command_to_handler_map = {
        {"pubsub", AdminRespHandlerType::aggregate_all_responses},
        {"keys", AdminRespHandlerType::aggregate_all_responses},
        {"slowlog", AdminRespHandlerType::aggregate_all_responses},
        {"client", AdminRespHandlerType::aggregate_all_responses},
        {"info", AdminRespHandlerType::aggregate_all_responses},
        {"script", AdminRespHandlerType::allresponses_mustbe_same},
        {"flushall", AdminRespHandlerType::allresponses_mustbe_same},
        {"config", AdminRespHandlerType::allresponses_mustbe_same},
        {"select", AdminRespHandlerType::allresponses_mustbe_same},
        {"publish", AdminRespHandlerType::singleshardresponse},
        {"cluster", AdminRespHandlerType::singleshardresponse},
        {"flushdb", AdminRespHandlerType::allresponses_mustbe_same},
        {"xadd", AdminRespHandlerType::singleshardresponse},
        {"xread", AdminRespHandlerType::singleshardresponse},
        {"xlen", AdminRespHandlerType::singleshardresponse},
        {"xdel", AdminRespHandlerType::singleshardresponse},
        {"xtrim", AdminRespHandlerType::singleshardresponse},
        {"xrange", AdminRespHandlerType::singleshardresponse},
        {"xrevrange", AdminRespHandlerType::singleshardresponse},
        {"rename", AdminRespHandlerType::singleshardresponse},
        {"unwatch", AdminRespHandlerType::allresponses_mustbe_same},
        // Add more mappings as needed
    };

    // Look up the command name in the map
    auto it = command_to_handler_map.find(command_name);
    if (it != command_to_handler_map.end()) {
        // Return the corresponding response handler type if found
        return it->second;
    } else {
        // If the command name is not found, check if it belongs to subscription commands
        if (Common::Redis::SupportedCommands::subscriptionCommands().contains(command_name)) {
            return AdminRespHandlerType::aggregate_all_responses;
        } else {
            // Otherwise, return the default response handler type
            return AdminRespHandlerType::response_handler_none;
        }
    }
}
int32_t getShardIndex(const std::string command, int32_t requestsCount,int32_t redisShardsCount) {
  int32_t shard_index = -1;
  //This need optimisation , generate random seed only once per thread
  srand(time(nullptr));

  bool isBlockingCommand = Common::Redis::SupportedCommands::blockingCommands().contains(command);
  bool isAllShardCommand = Common::Redis::SupportedCommands::allShardCommands().contains(command);
  
  if (!isBlockingCommand && !isAllShardCommand && requestsCount == 1 ){
    // Send request to a random shard so that we donot allways send to the same shard
    shard_index = rand() % redisShardsCount;
  }
  return shard_index;
}

bool checkIfAdminCommandSupported(const std::string command,const std::string subcommand){
  if (command == "client" && subcommand != "list"){
    return false;
  }else if (command == "cluster" && ((subcommand != "keyslot") && (subcommand != "slots"))){
    return false;
  }else {
    return true;
  }
}

int32_t getRequestCount(const std::string command_name, int32_t redisShardsCount) {
  int32_t requestsCount = 1;
  if (Common::Redis::SupportedCommands::allShardCommands().contains(command_name)) {
    requestsCount = redisShardsCount;
  }

  return requestsCount;
}

Common::Redis::Client::PoolRequest* makeNoKeyRequest(
    const RouteSharedPtr& route, int32_t shard_index, Common::Redis::RespValueConstSharedPtr incoming_request, ConnPool::PoolCallbacks& callbacks,
    Common::Redis::Client::Transaction& transaction) {
    std::string key = std::string();
  Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl* req_instance =
        dynamic_cast<Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl*>(
            route->upstream(key).get());
  auto handler = req_instance->makeRequestNoKey(shard_index, ConnPool::RespVariant(incoming_request),
                                                       callbacks, transaction);
    return handler;
}

Common::Redis::Client::PoolRequest* 
makeBlockingRequest(const RouteSharedPtr& route, int32_t shard_index, const std::string& key,Common::Redis::RespValueConstSharedPtr incoming_request, ConnPool::PoolCallbacks& callbacks,
  Common::Redis::Client::Transaction& transaction) {
  Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl* req_instance =
      dynamic_cast<Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl*>(route->upstream(key).get());
  auto handler = req_instance->makeBlockingClientRequest(shard_index,key,ConnPool::RespVariant(incoming_request),
                                                       callbacks, transaction);
  return handler;
}

bool 
makePubSubRequest(const RouteSharedPtr& route, int32_t shard_index, const std::string& key,Common::Redis::RespValueConstSharedPtr incoming_request,Common::Redis::Client::Transaction& transaction) {
  Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl* req_instance =
      dynamic_cast<Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl*>(route->upstream(key).get());
  auto isSuccess = req_instance->makePubSubRequest(shard_index,key,ConnPool::RespVariant(incoming_request), transaction);
  return isSuccess;
}

/**
 * Make request and maybe mirror the request based on the mirror policies of the route.
 * @param route supplies the route matched with the request.
 * @param command supplies the command of the request.
 * @param key supplies the key of the request.
 * @param incoming_request supplies the request.
 * @param callbacks supplies the request completion callbacks.
 * @param transaction supplies the transaction info of the current connection.
 * @return PoolRequest* a handle to the active request or nullptr if the request could not be made
 *         for some reason.
 */
Common::Redis::Client::PoolRequest*
makeFragmentedRequest(const RouteSharedPtr& route, const std::string& command,
                      const std::string& key, const Common::Redis::RespValue& incoming_request,
                      ConnPool::PoolCallbacks& callbacks,
                      Common::Redis::Client::Transaction& transaction) {
  auto handler = route->upstream(command)->makeRequest(key, ConnPool::RespVariant(incoming_request),
                                                       callbacks, transaction);
  if (handler) {
    for (auto& mirror_policy : route->mirrorPolicies()) {
      if (mirror_policy->shouldMirror(command)) {
        mirror_policy->upstream()->makeRequest(key, ConnPool::RespVariant(incoming_request),
                                               null_pool_callbacks, transaction);
      }
    }
  }
  return handler;
}

// Send a string response downstream.
void localResponse(SplitCallbacks& callbacks, std::string response) {
  Common::Redis::RespValuePtr res(new Common::Redis::RespValue());
  res->type(Common::Redis::RespType::SimpleString);
  res->asString() = response;
  callbacks.onResponse(std::move(res));
}
} // namespace

void SplitRequestBase::onWrongNumberOfArguments(SplitCallbacks& callbacks,
                                                const Common::Redis::RespValue& request) {
  callbacks.onResponse(Common::Redis::Utility::makeError(
      fmt::format("wrong number of arguments for '{}' command", request.asArray()[0].asString())));
}

void SplitRequestBase::updateStats(const bool success) {
  if (success) {
    command_stats_.success_.inc();
  } else {
    command_stats_.error_.inc();
  }
  if (command_latency_ != nullptr) {
    command_latency_->complete();
  }
}

SingleServerRequest::~SingleServerRequest() { ASSERT(!handle_); }

void SingleServerRequest::onResponse(Common::Redis::RespValuePtr&& response) {
  handle_ = nullptr;
  updateStats(true);
  callbacks_.onResponse(std::move(response));
}

void SingleServerRequest::onFailure() { onFailure(Response::get().UpstreamFailure); }

void SingleServerRequest::onFailure(std::string error_msg) {
  handle_ = nullptr;
  updateStats(false);
  callbacks_.transaction().should_close_ = true;
  callbacks_.onResponse(Common::Redis::Utility::makeError(error_msg));
}

void SingleServerRequest::cancel() {
  handle_->cancel();
  handle_ = nullptr;
}

SplitRequestPtr ErrorFaultRequest::create(SplitCallbacks& callbacks, CommandStats& command_stats,
                                          TimeSource& time_source, bool delay_command_latency,
                                          const StreamInfo::StreamInfo&) {
  std::unique_ptr<ErrorFaultRequest> request_ptr{
      new ErrorFaultRequest(callbacks, command_stats, time_source, delay_command_latency)};

  request_ptr->onFailure(Common::Redis::FaultMessages::get().Error);
  command_stats.error_fault_.inc();
  return nullptr;
}

std::unique_ptr<DelayFaultRequest>
DelayFaultRequest::create(SplitCallbacks& callbacks, CommandStats& command_stats,
                          TimeSource& time_source, Event::Dispatcher& dispatcher,
                          std::chrono::milliseconds delay, const StreamInfo::StreamInfo&) {
  return std::make_unique<DelayFaultRequest>(callbacks, command_stats, time_source, dispatcher,
                                             delay);
}

void DelayFaultRequest::onResponse(Common::Redis::RespValuePtr&& response) {
  response_ = std::move(response);
  delay_timer_->enableTimer(delay_);
}

void DelayFaultRequest::onDelayResponse() {
  command_stats_.delay_fault_.inc();
  command_latency_->complete(); // Complete latency of the command stats of the wrapped request
  callbacks_.onResponse(std::move(response_));
}

void DelayFaultRequest::cancel() { delay_timer_->disableTimer(); }

SplitRequestPtr SimpleRequest::create(Router& router,
                                      Common::Redis::RespValuePtr&& incoming_request,
                                      SplitCallbacks& callbacks, CommandStats& command_stats,
                                      TimeSource& time_source, bool delay_command_latency,
                                      const StreamInfo::StreamInfo& stream_info) {
  std::unique_ptr<SimpleRequest> request_ptr{
      new SimpleRequest(callbacks, command_stats, time_source, delay_command_latency)};
  const auto route = router.upstreamPool(incoming_request->asArray()[1].asString(), stream_info);
  if (route) {
    Common::Redis::RespValueSharedPtr base_request = std::move(incoming_request);
    request_ptr->handle_ = makeSingleServerRequest(
        route, base_request->asArray()[0].asString(), base_request->asArray()[1].asString(),
        base_request, *request_ptr, callbacks.transaction());
  } else {
    ENVOY_LOG(debug, "route not found: '{}'", incoming_request->toString());
  }

  if (!request_ptr->handle_) {
    command_stats.error_.inc();
    callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
    return nullptr;
  }

  return request_ptr;
}

SplitRequestPtr EvalRequest::create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                    SplitCallbacks& callbacks, CommandStats& command_stats,
                                    TimeSource& time_source, bool delay_command_latency,
                                    const StreamInfo::StreamInfo& stream_info) {
  // EVAL looks like: EVAL script numkeys key [key ...] arg [arg ...]
  // Ensure there are at least three args to the command or it cannot be hashed.
  if (incoming_request->asArray().size() < 4) {
    onWrongNumberOfArguments(callbacks, *incoming_request);
    command_stats.error_.inc();
    return nullptr;
  }

  std::unique_ptr<EvalRequest> request_ptr{
      new EvalRequest(callbacks, command_stats, time_source, delay_command_latency)};

  const auto route = router.upstreamPool(incoming_request->asArray()[3].asString(), stream_info);
  if (route) {
    Common::Redis::RespValueSharedPtr base_request = std::move(incoming_request);
    request_ptr->handle_ = makeSingleServerRequest(
        route, base_request->asArray()[0].asString(), base_request->asArray()[3].asString(),
        base_request, *request_ptr, callbacks.transaction());
  }

  if (!request_ptr->handle_) {
    command_stats.error_.inc();
    callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
    return nullptr;
  }

  return request_ptr;
}

FragmentedRequest::~FragmentedRequest() {
#ifndef NDEBUG
  for (const PendingRequest& request : pending_requests_) {
    ASSERT(!request.handle_);
  }
#endif
}

void FragmentedRequest::cancel() {
  for (PendingRequest& request : pending_requests_) {
    if (request.handle_) {
      request.handle_->cancel();
      request.handle_ = nullptr;
    }
  }
}

void FragmentedRequest::onChildFailure(uint32_t index) {
  onChildResponse(Common::Redis::Utility::makeError(Response::get().UpstreamFailure), index);
}


AdministrationRequest::~AdministrationRequest() {
#ifndef NDEBUG
  for (const PendingRequest& request : pending_requests_) {
    ASSERT(!request.handle_);
  }
#endif
}

void AdministrationRequest::cancel() {
  for (PendingRequest& request : pending_requests_) {
    if (request.handle_) {
      request.handle_->cancel();
      request.handle_ = nullptr;
    }
  }
}
void AdministrationRequest::onSingleShardResponseFailure(int32_t reqindex,int32_t shardindex) {
  updateStats(false);
  onSingleShardresponse(Common::Redis::Utility::makeError(Response::get().UpstreamFailure), reqindex,shardindex);
}

void AdministrationRequest::onallChildResponseSameFailure(int32_t reqindex,int32_t shardindex) {
  updateStats(false);
  onAllChildResponseSame(Common::Redis::Utility::makeError(Response::get().UpstreamFailure), reqindex,shardindex);
}

void AdministrationRequest::onallChildRespAgrregateFail(int32_t reqindex,int32_t shardindex) {
  updateStats(false);
  onallChildRespAgrregate(Common::Redis::Utility::makeError(Response::get().UpstreamFailure), reqindex,shardindex);
}

SplitRequestPtr mgmtNoKeyRequest::create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                    SplitCallbacks& callbacks, CommandStats& command_stats,
                                    TimeSource& time_source, bool delay_command_latency,
                                    const StreamInfo::StreamInfo& stream_info) {
  std::unique_ptr<mgmtNoKeyRequest> request_ptr{
      new mgmtNoKeyRequest(callbacks, command_stats, time_source, delay_command_latency)};
  std::string key = std::string();
  int32_t redisShardsCount=0;
  int32_t requestsCount=1;
  int32_t shard_index=0;
  bool iserror = false;
  std::string firstarg = std::string();

  std::string command_name = absl::AsciiStrToLower(incoming_request->asArray()[0].asString());
  if (incoming_request->asArray().size() > 1) {
    firstarg = absl::AsciiStrToLower(incoming_request->asArray()[1].asString());
  }else{
    firstarg = "";
  }
  
  if (!checkIfAdminCommandSupported(command_name,firstarg)){
    ENVOY_LOG(debug, "this admin command is not supported: '{}'", incoming_request->toString());
      callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().InvalidRequest));
      return nullptr;
  }

  const auto& route = router.upstreamPool(key, stream_info);

  if (route) {
    Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl* instance =
        dynamic_cast<Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl*>(
            route->upstream(key).get());
    redisShardsCount = instance->getRedisShardsCount();
    if (redisShardsCount <= 0){
      ENVOY_LOG(debug, "redisShardsCount not found: '{}'", incoming_request->toString());
      callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
      return nullptr;
    }
  }
  else{
    ENVOY_LOG(debug, "route not found: '{}'", incoming_request->toString());
    callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
    return nullptr;
  }
  requestsCount = getRequestCount(command_name,redisShardsCount);
  request_ptr->num_pending_responses_ = requestsCount;
  request_ptr->pending_requests_.reserve(request_ptr->num_pending_responses_);

    // Identify the response type based on the subcommand , needs to be written very clean 
    // along with subcommand identification for each command
    // For now keeping it ugly like this and assuming we only add support for script comand.
  Common::Redis::RespValueSharedPtr base_request = std::move(incoming_request);
  //reserve memory for list of responses if we have more than 1 request
  if(requestsCount >1){
    request_ptr->pending_responses_.reserve(request_ptr->num_pending_responses_);
  }
    
  for (int32_t i = 0; i < request_ptr->num_pending_responses_; i++) {
    shard_index=getShardIndex(command_name,requestsCount,redisShardsCount);
    if(shard_index < 0){
      shard_index = i;
    }
    request_ptr->pending_requests_.emplace_back(*request_ptr, i, shard_index, command_name, firstarg, getresponseHandlerType(command_name));
    PendingRequest& pending_request = request_ptr->pending_requests_.back();
    pending_request.handle_ = nullptr;
    
    pending_request.handle_= makeNoKeyRequest(route, shard_index,base_request, pending_request, callbacks.transaction());

    if (!pending_request.handle_) {
      ENVOY_LOG(debug, "Error in  makeRequestNoKey : '{}', to shard index: '{}'", incoming_request->toString(),shard_index);
      pending_request.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
      iserror = true;
      break;
    }
  }

  if (request_ptr->num_pending_responses_ > 0  && !iserror) {
    return request_ptr;
  }

  return nullptr;
}

SplitRequestPtr MGETRequest::create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                    SplitCallbacks& callbacks, CommandStats& command_stats,
                                    TimeSource& time_source, bool delay_command_latency,
                                    const StreamInfo::StreamInfo& stream_info) {
  std::unique_ptr<MGETRequest> request_ptr{
      new MGETRequest(callbacks, command_stats, time_source, delay_command_latency)};

  request_ptr->num_pending_responses_ = incoming_request->asArray().size() - 1;
  request_ptr->pending_requests_.reserve(request_ptr->num_pending_responses_);

  request_ptr->pending_response_ = std::make_unique<Common::Redis::RespValue>();
  request_ptr->pending_response_->type(Common::Redis::RespType::Array);
  std::vector<Common::Redis::RespValue> responses(request_ptr->num_pending_responses_);
  request_ptr->pending_response_->asArray().swap(responses);

  Common::Redis::RespValueSharedPtr base_request = std::move(incoming_request);
  for (uint32_t i = 1; i < base_request->asArray().size(); i++) {
    request_ptr->pending_requests_.emplace_back(*request_ptr, i - 1);
    PendingRequest& pending_request = request_ptr->pending_requests_.back();

    const auto route = router.upstreamPool(base_request->asArray()[i].asString(), stream_info);
    if (route) {
      // Create composite array for a single get.
      const Common::Redis::RespValue single_mget(
          base_request, Common::Redis::Utility::GetRequest::instance(), i, i);
      pending_request.handle_ =
          makeFragmentedRequest(route, "get", base_request->asArray()[i].asString(), single_mget,
                                pending_request, callbacks.transaction());
    }

    if (!pending_request.handle_) {
      pending_request.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
    }
  }

  if (request_ptr->num_pending_responses_ > 0) {
    return request_ptr;
  }

  return nullptr;
}

bool areAllResponsesSame(const std::vector<Common::Redis::RespValuePtr>& responses) {
    if (responses.empty()) {
        return true; 
    }

    const Common::Redis::RespValue* first_response = responses.front().get();
    for (const auto& response : responses) {
        if (!response || !first_response || *(response.get()) != *first_response) {
            return false;
        }
    }
    return true;
}

void parseInfoResponse(const std::string& inforesponse,Common::Redis::Utility::InfoCmdResponseProcessor& infoProcessor) {
    std::istringstream inputStream(inforesponse);
    std::string line;

    while (std::getline(inputStream, line)) {
        // Ignore lines that start with '#'
        if (!line.empty() && line[0] == '#') {
            continue;
        }

        std::size_t pos = line.find(':');
        if (pos != std::string::npos) {
            std::string key = line.substr(0, pos);
            std::string value = line.substr(pos + 1);

            // Trim whitespace from both ends
            key.erase(key.find_last_not_of(" \n\r\t") + 1);
            value.erase(0, value.find_first_not_of(" \n\r\t"));
            infoProcessor.processInfoCmdResponse(key, value);

        }
    }

    return;
}


void mgmtNoKeyRequest::onallChildRespAgrregate(Common::Redis::RespValuePtr&& value, int32_t reqindex, int32_t shardindex) {
  pending_requests_[reqindex].handle_ = nullptr;
  const auto& redisarg = pending_requests_[reqindex].redisarg_;
  const auto& rediscommand = pending_requests_[reqindex].rediscommand_;
  ENVOY_LOG(debug,"response recived for reqindex: '{}', shard Index: '{}'", reqindex,shardindex);
  if (reqindex >= static_cast<int32_t>(pending_responses_.size())) {
        // Resize the vector to accommodate the new index
        pending_responses_.resize(reqindex + 1);
  }

  if (value->type() == Common::Redis::RespType::Error){
    error_count_++;
    response_index_=reqindex;
  }
  // Move the value into the pending_responses at the specified index
  pending_responses_[reqindex] = std::move(value);

  ASSERT(num_pending_responses_ > 0);
  if (--num_pending_responses_ == 0) {
    if (error_count_ > 0 ){
      if (!pending_responses_.empty()) {
        Common::Redis::RespValuePtr response = std::move(pending_responses_[response_index_]);
        callbacks_.onResponse(std::move(response));
        pending_responses_.clear();
      }
    } else {
      bool positiveresponse = true;
      updateStats(error_count_ == 0);
      if (!pending_responses_.empty()) {
        if ( rediscommand == "info"){
          Common::Redis::RespValuePtr response = std::make_unique<Common::Redis::RespValue>();
          response->type(Common::Redis::RespType::BulkString);
          auto downstreamstats = callbacks_.transaction().getDownstreamCallback()->getDownStreamMetrics();
          Envoy::Extensions::NetworkFilters::Common::Redis::Utility::InfoCmdResponseProcessor infoProcessor(*downstreamstats);
          for (auto& resp : pending_responses_) {
            if (resp->type() == Common::Redis::RespType::BulkString) {
              parseInfoResponse(resp->asString(),infoProcessor);
            } else {
              positiveresponse = false;
              ENVOY_LOG(debug, "Error: Unexpected response Type , expected bulkstring");
              break;
            } 
          }
          if (!positiveresponse) {
            callbacks_.onResponse(Common::Redis::Utility::makeError(
                  fmt::format("unexpected response type received from upstream")));
          }else {
            response->asString() += infoProcessor.getInfoCmdResponseString();
            callbacks_.onResponse(std::move(response));
          }
          pending_responses_.clear();
        }
        if ( rediscommand == "pubsub" || rediscommand == "keys" || rediscommand == "slowlog"|| rediscommand == "client") {
              if ((redisarg == "numpat" || redisarg == "len") && (rediscommand == "pubsub" || rediscommand == "slowlog")) {
                  int sum = 0; 
                  Common::Redis::RespValuePtr response = std::make_unique<Common::Redis::RespValue>();
                  response->type(Common::Redis::RespType::Integer);
                  for (auto& resp : pending_responses_) {
                    if (resp->type() == Common::Redis::RespType::Integer) {
                      try {
                          int integerValue = resp->asInteger(); // Convert to integer
                          if (integerValue >= 0) {
                              sum += integerValue; // Add integer value to sum if it's non-negative
                          } else {
                              positiveresponse = false;
                              // Handle negative integer value
                              ENVOY_LOG(error, "Error: Negative integer value: {}", integerValue);
                              callbacks_.onResponse(Common::Redis::Utility::makeError(
                                fmt::format("negative or unexpected response received from upstream")));
                          }
                      } catch (const std::exception& e) {
                          positiveresponse = false;
                          // Handle conversion error
                          ENVOY_LOG(error, "Error converting integer: {}", e.what());
                          callbacks_.onResponse(Common::Redis::Utility::makeError(
                            fmt::format("negative or unexpected response received from upstream")));
                      }
                    }
                  }
                  if (positiveresponse) {
                    response->asInteger() = sum;
                    callbacks_.onResponse(std::move(response));
                    pending_responses_.clear();
                  }
              } else {
                  Common::Redis::RespValuePtr response = std::make_unique<Common::Redis::RespValue>();
                  Common::Redis::RespValue innerResponse;
                  response->type(Common::Redis::RespType::Array);

                  // Iterate through pending_responses_ and append non-empty responses to the array
                  if ( redisarg == "channels" || rediscommand == "keys" || redisarg == "get" ) {
                    std::unordered_set<std::string> distinctReponse; // Set to store unique elements 
                    // In Pubsub channels, we have a corner case where we need to remove duplicates from the response 
                    // since same channel (keyspace/keyevent) can be subscribed 
                    // to multiple shards and only unique should be showed.
                    for (auto& resp : pending_responses_) {
                        if (resp->type() == Common::Redis::RespType::Array) {
                            for (auto& elem : resp->asArray()) {
                                std::string str_elem = elem.asString(); // Convert RespValue to string
                                // Check if the element is already available
                                if (distinctReponse.find(str_elem) == distinctReponse.end()) {
                                    // If not seen, add it to the response array and store in unique_elements
                                    distinctReponse.insert(str_elem);
                                    response->asArray().emplace_back(std::move(elem));
                                }
                            }
                        }
                    }
                  }else if (rediscommand == "client" && redisarg == "list") {
                    for (auto& resp : pending_responses_) {
                        if (resp->type() == Common::Redis::RespType::BulkString) {
                          innerResponse = *resp;
                          response->asArray().emplace_back(std::move(innerResponse));
                        } 
                      }
                  }else if (redisarg == "numsub"){
                      std::unordered_map<std::string, int> subscriberCounts;
                      for (auto& resp : pending_responses_) {
                        if (resp->type() == Common::Redis::RespType::Array) {
                            innerResponse = std::move(*resp);
                            for (size_t i = 0; i < innerResponse.asArray().size(); i += 2) {
                              auto& channelResp = innerResponse.asArray()[i];
                              auto& countResp = innerResponse.asArray()[i + 1];
                              auto channel = channelResp.asString();
                              auto count = countResp.asInteger();
                              subscriberCounts[channel] += count;
                          }
                        }
                      }
                      std::vector<Envoy::Extensions::NetworkFilters::Common::Redis::RespValue> responseValues;
                      Envoy::Extensions::NetworkFilters::Common::Redis::RespValue channelResp;
                      channelResp.type(Common::Redis::RespType::BulkString);
                      Envoy::Extensions::NetworkFilters::Common::Redis::RespValue countResp;
                      countResp.type(Common::Redis::RespType::Integer);
                                                    

                      for (const auto& [channel, count] : subscriberCounts) {
                          // Create RespValue objects for channel and count
                          channelResp.asString() = channel;                          
                          countResp.asInteger() = count;

                          // Add channel and count directly to response array
                          responseValues.emplace_back(std::move(channelResp));
                          responseValues.emplace_back(std::move(countResp));
                      }

                      // Create a RespValue object representing an array containing all channel-count pairs
                      Envoy::Extensions::NetworkFilters::Common::Redis::RespValue responseArray;
                      responseArray.type(Common::Redis::RespType::Array);
                      for (auto& value : responseValues) {
                          responseArray.asArray().emplace_back(std::move(value));
                      }

                      // Assign responseArray to response
                      response = std::make_unique<Envoy::Extensions::NetworkFilters::Common::Redis::RespValue>(std::move(responseArray));

                      // Now you can get the string representation of the response
                      std::string responseString = response->toString();
                  } else if ( redisarg == "reset" ) {
                      if(areAllResponsesSame(pending_responses_)) {
                        response = std::move(pending_responses_[0]);
                      } else {
                        positiveresponse = false;
                        ENVOY_LOG(debug, "all response not same: '{}'", pending_responses_[0]->toString());
                        callbacks_.onResponse(Common::Redis::Utility::makeError(
                            fmt::format("all responses not same")));
                        if (!pending_responses_.empty()) {    
                          pending_responses_.clear();
                        }
                      }
                  }
                  if (positiveresponse) {
                    callbacks_.onResponse(std::move(response));
                    pending_responses_.clear();
                  }
              }
        }
      }
    }
  }  
}

void mgmtNoKeyRequest::onSingleShardresponse(Common::Redis::RespValuePtr&& value, int32_t reqindex, int32_t shardindex) {
  pending_requests_[reqindex].handle_ = nullptr;

  ENVOY_LOG(debug,"response recived for reqindex: '{}', shard Index: '{}'", reqindex,shardindex);
  ENVOY_LOG(debug, "response: {}", value->toString());
  updateStats(true);
  callbacks_.onResponse(std::move(value));
  pending_responses_.clear();
}

void mgmtNoKeyRequest::onAllChildResponseSame(Common::Redis::RespValuePtr&& value, int32_t reqindex, int32_t shardindex) {
  pending_requests_[reqindex].handle_ = nullptr;

  if (reqindex >= static_cast<int32_t>(pending_responses_.size())) {
        // Resize the vector to accommodate the new reqindex
        pending_responses_.resize(reqindex + 1);
  }
  ENVOY_LOG(debug,"response recived for reqindex: '{}', shard index:'{}'", reqindex,shardindex);

  if (value->type() == Common::Redis::RespType::Error){
    error_count_++;
    response_index_=reqindex;
  }
  // Move the value into the pending_responses at the specified reqindex
  pending_responses_[reqindex] = std::move(value);

  ASSERT(num_pending_responses_ > 0);
  if (--num_pending_responses_ == 0) {
    if (error_count_ > 0 ){
      updateStats(false);
      if (!pending_responses_.empty()) {
        ENVOY_LOG(debug, "Error Response received: '{}'", pending_responses_[response_index_]->toString());
        Common::Redis::RespValuePtr response = std::move(pending_responses_[response_index_]);
        callbacks_.onResponse(std::move(response));
        pending_responses_.clear();
      }
    } else if(! areAllResponsesSame(pending_responses_)) {
      updateStats(false);
      ENVOY_LOG(debug, "all response not same: '{}'", pending_responses_[0]->toString());
      callbacks_.onResponse(Common::Redis::Utility::makeError(
          fmt::format("all responses not same")));
      if (!pending_responses_.empty())    
        pending_responses_.clear();
    }else {
      updateStats(true);
      if (!pending_responses_.empty()) {
      Common::Redis::RespValuePtr response = std::move(pending_responses_[0]);
      ENVOY_LOG(debug, "response: {}", response->toString()); 
      callbacks_.onResponse(std::move(response));
      pending_responses_.clear();
      }
    }
  }
}

SplitRequestPtr BlockingClientRequest::create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                    SplitCallbacks& callbacks, CommandStats& command_stats,
                                    TimeSource& time_source, bool delay_command_latency,
                                    const StreamInfo::StreamInfo& stream_info){
  // For blocking requests which operate on a single key, we can hash the key to a single 
  //must send shard index as negative to indicate that it is a blocking request that acts on key
  std::string command_name = absl::AsciiStrToLower(incoming_request->asArray()[0].asString());
  int32_t shard_index=getShardIndex(command_name,1,1);
  Common::Redis::Client::Transaction& transaction = callbacks.transaction();

  std::unique_ptr<BlockingClientRequest> request_ptr{
      new BlockingClientRequest(callbacks, command_stats, time_source, delay_command_latency)};
  std::string key = absl::AsciiStrToLower(incoming_request->asArray()[1].asString());

 if (transaction.active_ ){
    // when we are in blocking command, we cannnot accept any other commands
    if (transaction.isBlockingCommand()) {
        callbacks.onResponse(
            Common::Redis::Utility::makeError(fmt::format("not supported command in blocked state")));
        return nullptr;
    } else if (transaction.isTransactionMode()) {
      // subscription commands are not supported in transaction mode, we will not get here , but just in case
        callbacks.onResponse(Common::Redis::Utility::makeError(fmt::format("not supported when in transaction mode")));
        transaction.should_close_ = true;
        return nullptr;
    }
  }else {
    if (Common::Redis::SupportedCommands::blockingCommands().contains(command_name)){
      transaction.clients_.resize(1);
      transaction.setBlockingCommand();
      transaction.start();
    }else{
      ENVOY_LOG(debug, "unexpected command : '{}'", command_name);
      callbacks.onResponse(Common::Redis::Utility::makeError(fmt::format("unexpected error")));
      return nullptr;
    }
  }
  const auto route = router.upstreamPool(incoming_request->asArray()[1].asString(), stream_info);
  if (route) {
      Common::Redis::RespValueSharedPtr base_request = std::move(incoming_request);
    request_ptr->handle_ = makeBlockingRequest(
        route,shard_index,key,base_request, *request_ptr, callbacks.transaction());
  } else {
    ENVOY_LOG(debug, "route not found: '{}'", incoming_request->toString());
  }
  if (!request_ptr->handle_) {
    command_stats.error_.inc();
    transaction.should_close_ = true;
    callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
    return nullptr;
  }
  transaction.connection_established_=true;
  transaction.should_close_ = false;
  return request_ptr;
}

bool isKeyspaceArgument(const std::string& argument) {
  std::string keyspacepattern = "__keyspace@0__";
  std::string keyeventpattern = "__keyevent@0__";
  if (argument.find(keyspacepattern) == 0 || argument.find(keyeventpattern) == 0) {
    return true;
  }
  return false;
}

// Generic function to build an array with bulkstring
void addBulkString(Common::Redis::RespValue& requestArray, const std::string& value) {
    Common::Redis::RespValue element;
    element.type(Common::Redis::RespType::BulkString);
    element.asString() = value;
    requestArray.asArray().emplace_back(std::move(element));
}

SplitRequestPtr PubSubRequest::create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                    SplitCallbacks& callbacks, CommandStats& command_stats,
                                    TimeSource& time_source, bool delay_command_latency,
                                    const StreamInfo::StreamInfo& stream_info){

  Common::Redis::Client::Transaction& transaction = callbacks.transaction();
  std::string command_name = absl::AsciiStrToLower(incoming_request->asArray()[0].asString());
  (void)time_source;    // Mark time_source as unused
  (void)delay_command_latency; // Mark delay_command_latency as unused
  std::string key = std::string();
  int32_t redisShardsCount=0;

  bool singleShardRequest = false;
  bool allShardsRequest = false;
  bool allShardwithSingleShardRequest = false;
  Common::Redis::RespValueSharedPtr base_request;
  Common::Redis::RespValueSharedPtr keyspaceRequest;
  Common::Redis::RespValue keyspaceRequestArray;
  
  // Analyze request to get the proper request type
  if (command_name == "subscribe" || command_name == "psubscribe") {
    bool containsKeyspaceChannel = false;
    bool containsNormalChannel = false;
    for (size_t i = 1; i < incoming_request->asArray().size(); i++) {
      std::string channel = absl::AsciiStrToLower(incoming_request->asArray()[i].asString());
      if (isKeyspaceArgument(channel)) {
        ENVOY_LOG(debug, "keyspace command: '{}'", channel);
        containsKeyspaceChannel = true;
      } else {
        containsNormalChannel = true;
      }
    }
    
  allShardsRequest = containsKeyspaceChannel && !containsNormalChannel;
  allShardwithSingleShardRequest = containsKeyspaceChannel && containsNormalChannel;
  singleShardRequest = !containsKeyspaceChannel && containsNormalChannel;

  ENVOY_LOG(debug,"PubSubRequest Request type ++ allShardsRequest: '{}' -- allShardwithSingleShardRequest: '{}' -- singleShardRequest: '{}'",allShardsRequest,allShardwithSingleShardRequest,singleShardRequest);


  } else if (command_name == "unsubscribe" || command_name == "punsubscribe") {
    allShardsRequest = true;
  }

  const auto& route = router.upstreamPool(key, stream_info);
  if (route) {
    Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl* instance =
        dynamic_cast<Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl*>(
            route->upstream(key).get());
    redisShardsCount = instance->getRedisShardsCount();
    if (redisShardsCount <= 0){
      ENVOY_LOG(debug, "redisShardsCount not found: '{}'", incoming_request->toString());
      callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
      return nullptr;
    }
  }
  else{
    ENVOY_LOG(debug, "route not found: '{}'", incoming_request->toString());
    callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
    return nullptr;
  }

  std::shared_ptr<PubSubMessageHandler> PubSubMsghandler; 
  if (transaction.active_) {
    if (transaction.isSubscribedMode()) {
        if (!Common::Redis::SupportedCommands::subcrStateallowedCommands().contains(command_name)) {
            callbacks.onResponse(
                Common::Redis::Utility::makeError("Not supported command in subscribe state"));
        } else if (command_name == "quit") {
            transaction.should_close_ = true;
            transaction.subscribed_client_shard_index_ = -1;
            localResponse(callbacks, "OK");
            transaction.setPubSubCallback(nullptr);
            return nullptr;
        }
    } else if (transaction.isTransactionMode()) {
        callbacks.onResponse(
            Common::Redis::Utility::makeError("Not supported when in transaction mode"));
        transaction.should_close_ = true;
        transaction.setPubSubCallback(nullptr);
        return nullptr;
    }
}else {
  PubSubMsghandler = std::make_shared<PubSubMessageHandler>(transaction.getDownstreamCallback());
    if (Common::Redis::SupportedCommands::subcrStateEnterCommands().contains(command_name)){
      auto pubsubCallbacksHandler = std::static_pointer_cast<Common::Redis::Client::PubsubCallbacks>(PubSubMsghandler);
      transaction.setPubSubCallback(std::move(pubsubCallbacksHandler));
      transaction.clients_.resize(redisShardsCount);
      transaction.enterSubscribedMode();
      transaction.start();
    }else{
      ENVOY_LOG(debug, "not yet in subscription mode, must be in subscr mode first: '{}'", command_name);
      callbacks.onResponse(Common::Redis::Utility::makeError(fmt::format("not yet in subcribe mode")));
      return nullptr;
    }
  }

  auto makeRequest = [&](int32_t shard_index, Common::Redis::RespValueSharedPtr& base_request) -> bool {
    transaction.current_client_idx_ = shard_index;
    auto isSuccess = makePubSubRequest(route, shard_index, key, base_request, callbacks.transaction());
    if (!isSuccess) {
        command_stats.error_.inc();
        transaction.should_close_ = true;
        transaction.subscribed_client_shard_index_ = -1;
        callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
        return false;
    }
    return true;
  };

  if (singleShardRequest) {
    Common::Redis::RespValueSharedPtr base_request = std::move(incoming_request);
    if (transaction.subscribed_client_shard_index_ == -1) {
      transaction.subscribed_client_shard_index_ = getShardIndex(command_name, 1, redisShardsCount);
      PubSubMsghandler->setShardIndex(transaction.subscribed_client_shard_index_);
    }
    if (!makeRequest(transaction.subscribed_client_shard_index_, base_request)){
         ENVOY_LOG(debug,"makerequest failed for singleshard request");
        transaction.setPubSubCallback(nullptr);
        return nullptr;
    }
  }

  if (allShardsRequest) {
    Common::Redis::RespValueSharedPtr base_request = std::move(incoming_request);
    if (transaction.subscribed_client_shard_index_ == -1) {
      transaction.subscribed_client_shard_index_ = getShardIndex(command_name, 1, redisShardsCount);
      PubSubMsghandler->setShardIndex(transaction.subscribed_client_shard_index_);
    }
    
    for (int32_t i = 0; i < redisShardsCount; i++) {
        if (!makeRequest(i, base_request)){
          ENVOY_LOG(debug,"makerequest failed for allShardsRequest for shardIndex'{}'",i);
          transaction.setPubSubCallback(nullptr);
          return nullptr;
        }
            
    }
  }

  if (allShardwithSingleShardRequest) { 
    keyspaceRequestArray.type(Common::Redis::RespType::Array);
    addBulkString(keyspaceRequestArray, command_name);
    for (size_t i = 1; i < incoming_request->asArray().size(); i++) {
      std::string channel = absl::AsciiStrToLower(incoming_request->asArray()[i].asString());
      if (isKeyspaceArgument(channel)) {
          addBulkString(keyspaceRequestArray, channel);
      }
    }
    base_request = std::move(incoming_request);
    keyspaceRequest = std::make_unique<Common::Redis::RespValue>(keyspaceRequestArray);
    if (transaction.subscribed_client_shard_index_ == -1) {
      transaction.subscribed_client_shard_index_ = getShardIndex(command_name, 1, redisShardsCount);
      PubSubMsghandler->setShardIndex(transaction.subscribed_client_shard_index_);
    }
    
    for (int32_t i = 0; i < redisShardsCount; i++) {
      if (i == transaction.subscribed_client_shard_index_){
        if (!makeRequest(i, base_request)){
          ENVOY_LOG(debug,"makerequest failed for allShardwithSingleShardRequest for base request at shardIndex'{}'",i);
          transaction.setPubSubCallback(nullptr);
          return nullptr;
        }
            
      }else{
        if (!makeRequest(i, keyspaceRequest)){
          ENVOY_LOG(debug,"makerequest failed for allShardwithSingleShardRequest for keyspaceRequest at shardIndex'{}'",i);
          transaction.setPubSubCallback(nullptr);
          return nullptr;
        }
      }
    }
  }

  //Should we set to true irrespective of connpool handler returned
  transaction.connection_established_=true;
  transaction.should_close_ = false;
  Common::Redis::RespValuePtr nullresponse(new Common::Redis::RespValue());
  nullresponse->type(Common::Redis::RespType::Null);
  callbacks.onResponse(std::move(nullresponse));

  return nullptr;
}

void PubSubMessageHandler::handleChannelMessageCustom(Common::Redis::RespValuePtr&& value, int32_t clientIndex, int32_t shardIndex) {
  ENVOY_LOG(debug, "message received on channel '{}' on shardIndex : '{}'", value->toString(),shardIndex);

  if (value->type() != Common::Redis::RespType::Array ||
      value->asArray()[0].type() != Common::Redis::RespType::BulkString) {
    ENVOY_LOG(debug, "unexpected message format: '{}'", value->toString());
    return;
  }

  std::string message_type = value->asArray()[0].asString();
  if (message_type == "message" || message_type == "pmessage") {
      ENVOY_LOG(debug,"sending response downstream");
      downstream_callbacks_->sendResponseDownstream(std::move(value));
      return;
  }
  
  if (message_type == "subscribe" || message_type == "unsubscribe" || message_type == "psubscribe" || message_type == "punsubscribe") {
    if (clientIndex == shardIndex) {
      downstream_callbacks_->sendResponseDownstream(std::move(value));
      return;
    } else {
        ENVOY_LOG(debug, "Duplicate Message from shard index '{}', Ignoring!",shardIndex);
    }
  }else {
    ENVOY_LOG(debug, "unexpected message type: '{}'", value->toString());
  }
}

void PubSubMessageHandler::onFailure() {
  ENVOY_LOG(debug, "failure in pubsub message handler");
  downstream_callbacks_->onFailure();
}

void MGETRequest::onChildResponse(Common::Redis::RespValuePtr&& value, uint32_t index) {
  pending_requests_[index].handle_ = nullptr;

  pending_response_->asArray()[index].type(value->type());
  switch (value->type()) {
  case Common::Redis::RespType::Array:
  case Common::Redis::RespType::Integer:
  case Common::Redis::RespType::SimpleString:
  case Common::Redis::RespType::CompositeArray: {
    pending_response_->asArray()[index].type(Common::Redis::RespType::Error);
    pending_response_->asArray()[index].asString() = Response::get().UpstreamProtocolError;
    error_count_++;
    break;
  }
  case Common::Redis::RespType::Error: {
    error_count_++;
    FALLTHRU;
  }
  case Common::Redis::RespType::BulkString: {
    pending_response_->asArray()[index].asString().swap(value->asString());
    break;
  }
  case Common::Redis::RespType::Null:
    break;
  }

  ASSERT(num_pending_responses_ > 0);
  if (--num_pending_responses_ == 0) {
    updateStats(error_count_ == 0);
    ENVOY_LOG(debug, "response: '{}'", pending_response_->toString());
    callbacks_.onResponse(std::move(pending_response_));
  }
}

SplitRequestPtr MSETRequest::create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                    SplitCallbacks& callbacks, CommandStats& command_stats,
                                    TimeSource& time_source, bool delay_command_latency,
                                    const StreamInfo::StreamInfo& stream_info) {
  if ((incoming_request->asArray().size() - 1) % 2 != 0) {
    onWrongNumberOfArguments(callbacks, *incoming_request);
    command_stats.error_.inc();
    return nullptr;
  }
  std::unique_ptr<MSETRequest> request_ptr{
      new MSETRequest(callbacks, command_stats, time_source, delay_command_latency)};

  request_ptr->num_pending_responses_ = (incoming_request->asArray().size() - 1) / 2;
  request_ptr->pending_requests_.reserve(request_ptr->num_pending_responses_);

  request_ptr->pending_response_ = std::make_unique<Common::Redis::RespValue>();
  request_ptr->pending_response_->type(Common::Redis::RespType::SimpleString);

  Common::Redis::RespValueSharedPtr base_request = std::move(incoming_request);
  uint32_t fragment_index = 0;
  for (uint32_t i = 1; i < base_request->asArray().size(); i += 2) {
    request_ptr->pending_requests_.emplace_back(*request_ptr, fragment_index++);
    PendingRequest& pending_request = request_ptr->pending_requests_.back();

    const auto route = router.upstreamPool(base_request->asArray()[i].asString(), stream_info);
    if (route) {
      // Create composite array for a single set command.
      const Common::Redis::RespValue single_set(
          base_request, Common::Redis::Utility::SetRequest::instance(), i, i + 1);
      ENVOY_LOG(debug, "parallel set: '{}'", single_set.toString());
      pending_request.handle_ =
          makeFragmentedRequest(route, "set", base_request->asArray()[i].asString(), single_set,
                                pending_request, callbacks.transaction());
    }

    if (!pending_request.handle_) {
      pending_request.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
    }
  }

  if (request_ptr->num_pending_responses_ > 0) {
    return request_ptr;
  }

  return nullptr;
}

void MSETRequest::onChildResponse(Common::Redis::RespValuePtr&& value, uint32_t index) {
  pending_requests_[index].handle_ = nullptr;

  switch (value->type()) {
  case Common::Redis::RespType::SimpleString: {
    if (value->asString() == Response::get().OK) {
      break;
    }
    FALLTHRU;
  }
  default: {
    error_count_++;
    break;
  }
  }

  ASSERT(num_pending_responses_ > 0);
  if (--num_pending_responses_ == 0) {
    updateStats(error_count_ == 0);
    if (error_count_ == 0) {
      pending_response_->asString() = Response::get().OK;
      callbacks_.onResponse(std::move(pending_response_));
    } else {
      callbacks_.onResponse(Common::Redis::Utility::makeError(
          fmt::format("finished with {} error(s)", error_count_)));
    }
  }
}

Common::Redis::Client::PoolRequest* 
 makeScanRequest(const RouteSharedPtr& route, int32_t shard_index, 
                Common::Redis::RespValue& incoming_request, 
                ConnPool::PoolCallbacks& callbacks,
                Common::Redis::Client::Transaction& transaction) {
  std::string key = std::string();
  Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl* req_instance =
        dynamic_cast<Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl*>(
            route->upstream(key).get());
  auto handler = req_instance->makeRequestNoKey(shard_index, ConnPool::RespVariant(incoming_request),
                                                       callbacks, transaction);
  return handler;
}

bool requiresValue(const std::string& arg) {
    return (arg == "count" || arg == "match" || arg == "type");
}

void ScanRequest::onChildError(Common::Redis::RespValuePtr&& value) {
  // Setting null pointer to all pending requests
  for (auto& request : pending_requests_) {
    request.handle_ = nullptr;
  }
  // Clearing pending responses
  if (!pending_responses_.empty()) {
    pending_responses_.clear();
  }

  Common::Redis::RespValuePtr response_t = std::move(value);
  ENVOY_LOG(debug, "response: {}", response_t->toString());
  callbacks_.onResponse(std::move(response_t));

}

SplitRequestPtr ScanRequest::create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                    SplitCallbacks& callbacks, CommandStats& command_stats,
                                    TimeSource& time_source, bool delay_command_latency,
                                    const StreamInfo::StreamInfo& stream_info) {

  // SCAN looks like: SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
  // Ensure there are at least two args to the command or it cannot be scanned.
  // Also the number of arguments should be in even number, otherwise command is invalid
  if (incoming_request->asArray().size() < 2 || incoming_request->asArray().size() % 2 != 0) {
    onWrongNumberOfArguments(callbacks, *incoming_request);
    command_stats.error_.inc();
    return nullptr;
  }

  std::string key = std::string();
  int32_t shard_idx = 0; 
  int32_t numofRequests;

  Common::Redis::RespValue requestArray;
  requestArray.type(Common::Redis::RespType::Array);

  // Getting the right cursor before sending request
  // We are appending 4 digit custom value as shard index to the cursor returned from the previous scan request
  // If it is a first request, the received cursor will be sent directly without any preprocessing
  // Ex: If cursor length is more than 4 -> last four digits will be considered as index and the remaining will be taken as cursor
  // If cursor length is less than or equal to 4 -> all digits will be considered as cursor
  std::string cursor = incoming_request->asArray()[1].asString();
  if (cursor.length() > 4) {
    std::string index = cursor.substr(cursor.length() - 4);
    cursor = cursor.substr(0, cursor.length() - 4);
    shard_idx = std::stoi(index);
  }

  // Completely reconstructing the request to add/modify count since we can't override the incoming array directly
  // Add the command and cursor to the request array
  addBulkString(requestArray, "SCAN");
  addBulkString(requestArray, cursor);

  std::unique_ptr<ScanRequest> request_ptr{
      new ScanRequest(callbacks, command_stats, time_source, delay_command_latency)};

  // TODO : This value should be configurable through protobuf
  // Setting default count value to 1000
  request_ptr->resp_obj_count_ = "1000";

  // Iterate over the arguments modify count if necessary
  // We are setting 1000 as the default count value if the incoming request has more than that
  for (size_t i = 2; i < incoming_request->asArray().size(); ++i) {
      std::string arg = incoming_request->asArray()[i].asString();
      addBulkString(requestArray, arg);

      // Check if the argument requires a value
      if (requiresValue(arg)) {
          if (arg == "count") {
            std::string count = incoming_request->asArray()[++i].asString();
            if (std::stoi(count) < 1000) {
              addBulkString(requestArray, count);
              request_ptr->resp_obj_count_ = count;
            } else {
               // Override with default value only when the count value is greater than 1000
              addBulkString(requestArray, request_ptr->resp_obj_count_);
            }
            ++i;
          } else {
            // If the current argument requires a value, add it to the request array
            std::string value = incoming_request->asArray()[++i].asString();
            addBulkString(requestArray, value);
          }
      }
  }

  // If the "count" argument is not found, add it with the default value
  if (incoming_request->asArray().size() == 2) {
      addBulkString(requestArray, "count");
      addBulkString(requestArray, request_ptr->resp_obj_count_);
  }

  // caching the request and route for making child request from response
  request_ptr->request_ = requestArray;
  request_ptr->route_ = router.upstreamPool(key, stream_info);

  if (request_ptr->route_) {
    Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl* instance =
        dynamic_cast<Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl*>(
            request_ptr->route_->upstream(key).get());

    request_ptr->num_of_Shards_ = instance->getRedisShardsCount();
    if (request_ptr->num_of_Shards_ == 0 ) {
      callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
      return nullptr;
    }
  }
  else{
    callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
    return nullptr;
  }

  // If shard index is some random value, we are setting the shard to 0 to avoid crashing
  // This ensures scan always returns value.
  // If the current shard index is zero, we assume that we may need to send request to all the shards
  if (shard_idx > request_ptr->num_of_Shards_ || shard_idx == 0) {
    shard_idx = 0;
    numofRequests = request_ptr->num_of_Shards_;
  } else {
  // If we receive shard index other than zero, we assume that the request should be sent to current shard and all the remaining next shards.
    numofRequests = request_ptr->num_of_Shards_ - shard_idx;
  }

  // Reserving memory for pending_requests and pending_responses
  request_ptr->pending_requests_.reserve(numofRequests);
  request_ptr->pending_responses_.reserve(numofRequests);

  // Pending requests will be popped from the back so, pending request index is incremented and and shard index is decremented
  //pi => Pending request index
  //si => Shard index
  for(int32_t pi = 0, si = request_ptr->num_of_Shards_-1; pi < numofRequests && si >= shard_idx; pi++, si-- ) {
    request_ptr->pending_requests_.emplace_back(*request_ptr, pi, si);
  }

  PendingRequest& pending_request = request_ptr->pending_requests_.back();
  if (request_ptr->route_) {
    pending_request.handle_= makeScanRequest(request_ptr->route_, shard_idx, requestArray, pending_request, callbacks.transaction());
  }

  if (!pending_request.handle_) {
    pending_request.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
    return nullptr;
  }

  return request_ptr;
}

void ScanRequest::onChildResponse(Common::Redis::RespValuePtr&& value, int32_t index, int32_t shard_index) {
  
  if (value->type() == Common::Redis::RespType::Error){
      ENVOY_LOG(debug,"recived error for index: '{}'", shard_index);
      onChildError(std::move(value));
  } else {
    // Request handled successfully
    pending_requests_[index].handle_ = nullptr;
    // Moving the response to pending   response for doing validation later
    // Incrementing the number of pending responses, it will drained during the validation
    int64_t count = std::stoi(resp_obj_count_);

    // Checking the cursor and number of objects for child request
    std::string cursor = value->asArray()[0].asString();
    int64_t objectsReceived = value->asArray()[1].asArray().size();
    std::string objectsRemaining = std::to_string(count - objectsReceived);

    // Resizing pending repsonses array based on the incoming reponses
    if (index >= static_cast<int32_t>(pending_responses_.size())) {
          // Resize the vector to accommodate the new index
          pending_responses_.resize(index + 1);
    }
    ENVOY_LOG(debug,"response recived for index: '{}'", shard_index);

    pending_responses_[num_pending_responses_++] = std::move(value);
    bool send_response = true;

    // Following conditions needs to be satisfied for making a child request
    // 1) If cursor is zero, objects less than count
    //  1.1) If No more shards to scan, there won't be any child request
    //  1.2) If shards present, increment the shard index, update the count value, set cursor to 0 and send child request
    // 2) If cursor is not zero
    //  2.1) If objects returned is equal to count, there won't be any child request

    if (cursor == "0" && objectsReceived < count && shard_index+1 < num_of_Shards_) {
      // Popping the pending request, not needed anymore
      pending_requests_.pop_back();
      send_response = false;
      // Cursor is always zero for child request
      // Create the child request based on the original request
      Common::Redis::RespValue child_request = request_;

      // Set the cursor to "0" in the child request
      child_request.asArray()[1].asString() = "0";
      // Setting the new count for the child request
      for (size_t i = 2; i < child_request.asArray().size() - 1; ++i) {
        if (child_request.asArray()[i].asString() == "count") {
            child_request.asArray()[i + 1].asString() = objectsRemaining;
            break; // Stop searching after updating count
        }
      }
      ENVOY_LOG(debug, "Child request: {}", request_.toString());
      PendingRequest& pending_request = pending_requests_.back();
      pending_request.handle_= makeScanRequest(route_, pending_request.shard_index_, child_request, pending_request, callbacks_.transaction());
      if (!pending_request.handle_) {
        onChildError(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
      }
    }

    if (send_response) {
      // Setting null to the stale pending_requests_
      for (auto& request : pending_requests_) {
        request.handle_ = nullptr;
      }
      Common::Redis::RespValuePtr response = std::make_unique<Common::Redis::RespValue>();
      response->type(Common::Redis::RespType::Array);

      // Process cursor -> Append cursor with shard index so that next request will come to corresponding shard
      // if cursor is non zero, there are few elements remaining in the shard so set the index to the same shard
      // if cursor is zero, but the count is satisfied, the next request should go to next shard. so increament the index 
      if (cursor != "0" ) {
        std::string indexStr = std::to_string(shard_index);
        cursor += std::string(4 - indexStr.length(), '0') + indexStr;
      }
      if (cursor == "0" && shard_index+1 < num_of_Shards_) {
        std::string nextIndexStr = std::to_string(shard_index+1);
        cursor += std::string(4 - nextIndexStr.length(), '0') + nextIndexStr;
      }

      // Response array will be created in the following format
      // [0] -> latest scan cursor for next iteration
      // [1] -> array for returned objects from multiple pending responses

      // Setting the cursor from last response, since that is the latest one
      // We need iterate from here in the next request.
      Common::Redis::RespValue cstr;
      cstr.type(Common::Redis::RespType::BulkString);
      cstr.asString() = std::move(cursor);
      response->asArray().emplace_back(std::move(cstr));

      // Creating a temporary array to hold the objects from multiple pending responses
      Common::Redis::RespValue objArray;
      objArray.type(Common::Redis::RespType::Array);

      // Iterate through the pending responses
      for (size_t i = 0; i < pending_responses_.size(); ++i) {
        if (pending_responses_[i] != nullptr) {
          auto& resp = pending_responses_[i];
          if (resp->type() == Common::Redis::RespType::Array) {
              auto& obj = resp->asArray()[1];
              if (obj.type() == Common::Redis::RespType::Array) {
                // Iterate through the inner objects array and add its elements to the object array
                for (size_t k = 0; k < obj.asArray().size(); ++k) {
                  objArray.asArray().emplace_back(std::move(obj.asArray()[k]));
                }
              } else {
                ENVOY_LOG(debug, "received non array response for the objects while scanning");
              }
          } else {
            ENVOY_LOG(debug, "received non array response for the scan command");
          }
        } else {
          ENVOY_LOG(debug, "received null pointer as response from one of the shard");
        }
      }
      pending_responses_.clear();
      // Add the object array to the main response array as a nested array
      response->asArray().emplace_back(std::move(objArray));
      updateStats(error_count_ == 0);
      Common::Redis::RespValuePtr response_t = std::move(response);
      ENVOY_LOG(debug, "response: {}", response_t->toString()); 
      callbacks_.onResponse(std::move(response_t));
    }
  }
}

void ScanRequestBase::onChildFailure(int32_t reqindex,int32_t shardindex) {
  updateStats(false);
  onChildResponse(Common::Redis::Utility::makeError(Response::get().UpstreamFailure), reqindex,shardindex);
}

ScanRequestBase::~ScanRequestBase() {
#ifndef NDEBUG
  for (const PendingRequest& request : pending_requests_) {
    ASSERT(!request.handle_);
  }
#endif
}

void ScanRequestBase::cancel() {
  for (PendingRequest& request : pending_requests_) {
    if (request.handle_) {
      request.handle_->cancel();
      request.handle_ = nullptr;
    }
  }
}

SplitRequestPtr
SplitKeysSumResultRequest::create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                  SplitCallbacks& callbacks, CommandStats& command_stats,
                                  TimeSource& time_source, bool delay_command_latency,
                                  const StreamInfo::StreamInfo& stream_info) {
  std::unique_ptr<SplitKeysSumResultRequest> request_ptr{
      new SplitKeysSumResultRequest(callbacks, command_stats, time_source, delay_command_latency)};

  request_ptr->num_pending_responses_ = incoming_request->asArray().size() - 1;
  request_ptr->pending_requests_.reserve(request_ptr->num_pending_responses_);

  request_ptr->pending_response_ = std::make_unique<Common::Redis::RespValue>();
  request_ptr->pending_response_->type(Common::Redis::RespType::Integer);

  Common::Redis::RespValueSharedPtr base_request = std::move(incoming_request);
  for (uint32_t i = 1; i < base_request->asArray().size(); i++) {
    request_ptr->pending_requests_.emplace_back(*request_ptr, i - 1);
    PendingRequest& pending_request = request_ptr->pending_requests_.back();

    // Create the composite array for a single fragment.
    const Common::Redis::RespValue single_fragment(base_request, base_request->asArray()[0], i, i);
    ENVOY_LOG(debug, "parallel {}: '{}'", base_request->asArray()[0].asString(),
              single_fragment.toString());
    const auto route = router.upstreamPool(base_request->asArray()[i].asString(), stream_info);
    if (route) {
      pending_request.handle_ = makeFragmentedRequest(
          route, base_request->asArray()[0].asString(), base_request->asArray()[i].asString(),
          single_fragment, pending_request, callbacks.transaction());
    }

    if (!pending_request.handle_) {
      pending_request.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
    }
  }

  if (request_ptr->num_pending_responses_ > 0) {
    return request_ptr;
  }

  return nullptr;
}

void SplitKeysSumResultRequest::onChildResponse(Common::Redis::RespValuePtr&& value,
                                                uint32_t index) {
  pending_requests_[index].handle_ = nullptr;

  switch (value->type()) {
  case Common::Redis::RespType::Integer: {
    total_ += value->asInteger();
    break;
  }
  default: {
    error_count_++;
    break;
  }
  }

  ASSERT(num_pending_responses_ > 0);
  if (--num_pending_responses_ == 0) {
    updateStats(error_count_ == 0);
    if (error_count_ == 0) {
      pending_response_->asInteger() = total_;
      callbacks_.onResponse(std::move(pending_response_));
    } else {
      callbacks_.onResponse(Common::Redis::Utility::makeError(
          fmt::format("finished with {} error(s)", error_count_)));
    }
  }
}

SplitRequestPtr TransactionRequest::create(Router& router,
                                           Common::Redis::RespValuePtr&& incoming_request,
                                           SplitCallbacks& callbacks, CommandStats& command_stats,
                                           TimeSource& time_source, bool delay_command_latency,
                                           const StreamInfo::StreamInfo& stream_info) {
  Common::Redis::Client::Transaction& transaction = callbacks.transaction();
  std::string command_name = absl::AsciiStrToLower(incoming_request->asArray()[0].asString());

  // Within transactions we support simple commands and also other commands with a limitation such that it will all be handled only in a single shard.
  // So if this is not a transaction command or a simple command or transaction allowed cmmand, it is an error.
  if (Common::Redis::SupportedCommands::transactionCommands().count(command_name) == 0 &&
      Common::Redis::SupportedCommands::simpleCommands().count(command_name) == 0 && Common::Redis::SupportedCommands::transactionAllowedNonSimpleCommands().count(command_name) == 0) {
    callbacks.onResponse(Common::Redis::Utility::makeError(
        fmt::format("'{}' command is not supported within transaction",
                    incoming_request->asArray()[0].asString())));
    return nullptr;
  }

  // Start transaction on MULTI, and stop on EXEC/DISCARD.
  if (command_name == "multi") {
    // Check for nested MULTI commands.
    if (transaction.active_) {
      callbacks.onResponse(
          Common::Redis::Utility::makeError(fmt::format("MULTI calls can not be nested")));
      return nullptr;
    }
    transaction.start();
    transaction.enterTransactionMode();
    // Respond to MULTI locally.
    localResponse(callbacks, "OK");
    return nullptr;

  } else if (command_name == "exec" || command_name == "discard") {
    // Handle the case where we don't have an open transaction.
    if (transaction.active_ == false && !transaction.is_transaction_mode_) {
      callbacks.onResponse(Common::Redis::Utility::makeError(
          fmt::format("{} without MULTI", absl::AsciiStrToUpper(command_name))));
      return nullptr;
    }

    // Handle the case where the transaction is empty.
    if (transaction.key_.empty()) {
      if (command_name == "exec") {
        Common::Redis::RespValuePtr empty_array{new Common::Redis::Client::EmptyArray{}};
        callbacks.onResponse(std::move(empty_array));
      } else {
        localResponse(callbacks, "OK");
      }
      transaction.close();
      return nullptr;
    }

    // In all other cases we will close the transaction connection after sending the last command.
    transaction.should_close_ = true;
  }

  // When we receive the first command with a key we will set this key as our transaction
  // key, and then send a MULTI command to the node that handles that key.
  // The response for the MULTI command will be discarded since we pass the null_pool_callbacks
  // to the handler.

  RouteSharedPtr route;
  if (transaction.key_.empty()) {
    transaction.key_ = incoming_request->asArray()[1].asString();
    route = router.upstreamPool(transaction.key_, stream_info);
    Common::Redis::RespValueSharedPtr multi_request =
        std::make_shared<Common::Redis::Client::MultiRequest>();
    if (route) {
      // We reserve a client for the main connection and for each mirror connection.
      transaction.clients_.resize(1 + route->mirrorPolicies().size());
      makeSingleServerRequest(route, "MULTI", transaction.key_, multi_request, null_pool_callbacks,
                              callbacks.transaction());
      transaction.connection_established_ = true;
    }
  } else {
    route = router.upstreamPool(transaction.key_, stream_info);
  }

  std::unique_ptr<TransactionRequest> request_ptr{
      new TransactionRequest(callbacks, command_stats, time_source, delay_command_latency)};

  if (route) {
    Common::Redis::RespValueSharedPtr base_request = std::move(incoming_request);
    request_ptr->handle_ =
        makeSingleServerRequest(route, base_request->asArray()[0].asString(), transaction.key_,
                                base_request, *request_ptr, callbacks.transaction());
  }

  if (!request_ptr->handle_) {
    command_stats.error_.inc();
    callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
    return nullptr;
  }

  return request_ptr;
}

InstanceImpl::InstanceImpl(RouterPtr&& router, Stats::Scope& scope, const std::string& stat_prefix,
                           TimeSource& time_source, bool latency_in_micros,
                           Common::Redis::FaultManagerPtr&& fault_manager)
    : router_(std::move(router)), simple_command_handler_(*router_),
      eval_command_handler_(*router_), mget_handler_(*router_), mset_handler_(*router_),
      split_keys_sum_result_handler_(*router_),
      transaction_handler_(*router_),mgmt_nokey_request_handler_(*router_),scanrequest_handler_(*router_),subscription_handler_(*router_),
      blocking_client_request_handler_(*router_),stats_{ALL_COMMAND_SPLITTER_STATS(POOL_COUNTER_PREFIX(scope, stat_prefix + "splitter."))},
      time_source_(time_source), fault_manager_(std::move(fault_manager)) {
  for (const std::string& command : Common::Redis::SupportedCommands::simpleCommands()) {
    addHandler(scope, stat_prefix, command, latency_in_micros, simple_command_handler_);
  }

  for (const std::string& command : Common::Redis::SupportedCommands::evalCommands()) {
    addHandler(scope, stat_prefix, command, latency_in_micros, eval_command_handler_);
  }

  for (const std::string& command :
       Common::Redis::SupportedCommands::hashMultipleSumResultCommands()) {
    addHandler(scope, stat_prefix, command, latency_in_micros, split_keys_sum_result_handler_);
  }

  addHandler(scope, stat_prefix, Common::Redis::SupportedCommands::mget(), latency_in_micros,
             mget_handler_);

  addHandler(scope, stat_prefix, Common::Redis::SupportedCommands::mset(), latency_in_micros,
             mset_handler_);
  
  addHandler(scope, stat_prefix, Common::Redis::SupportedCommands::scan(), latency_in_micros,
             scanrequest_handler_);

  for (const std::string& command : Common::Redis::SupportedCommands::transactionCommands()) {
    addHandler(scope, stat_prefix, command, latency_in_micros, transaction_handler_);
  }
  for (const std::string& command : Common::Redis::SupportedCommands::adminNokeyCommands()) {
    addHandler(scope, stat_prefix, command, latency_in_micros, mgmt_nokey_request_handler_);
  }
  for (const std::string& command : Common::Redis::SupportedCommands::subscriptionCommands()) {
    addHandler(scope, stat_prefix, command, latency_in_micros, subscription_handler_);
  }
  for (const std::string& command : Common::Redis::SupportedCommands::blockingCommands()) {
    addHandler(scope, stat_prefix, command, latency_in_micros, blocking_client_request_handler_);
  }
}

SplitRequestPtr InstanceImpl::makeRequest(Common::Redis::RespValuePtr&& request,
                                          SplitCallbacks& callbacks, Event::Dispatcher& dispatcher,
                                          const StreamInfo::StreamInfo& stream_info) {
  if ((request->type() != Common::Redis::RespType::Array) || request->asArray().empty()) {
    ENVOY_LOG(debug,"invalid request - not an array or empty");
    onInvalidRequest(callbacks);
    return nullptr;
  }

  for (const Common::Redis::RespValue& value : request->asArray()) {
    if (value.type() != Common::Redis::RespType::BulkString) {
      ENVOY_LOG(debug,"invalid request - not an array of bulk strings");
      onInvalidRequest(callbacks);
      return nullptr;
    }
  }

  std::string command_name = absl::AsciiStrToLower(request->asArray()[0].asString());

  if (command_name == Common::Redis::SupportedCommands::hello()) {
    // Respond to HELLO locally
    // Adding this before auth, since hello will be issued before auth command
    callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().UnKnownCommandHello));
    return nullptr;

  }

  if (command_name == Common::Redis::SupportedCommands::auth()) {
    if (request->asArray().size() < 2) {
      ENVOY_LOG(debug,"invalid request - not enough arguments for auth command");
      onInvalidRequest(callbacks);
      return nullptr;
    }
    if (request->asArray().size() == 3) {
      callbacks.onAuth(request->asArray()[1].asString(), request->asArray()[2].asString());
    } else {
      callbacks.onAuth(request->asArray()[1].asString());
    }

    return nullptr;
  }

  if (!callbacks.connectionAllowed()) {
    stats_.auth_failure_.inc();
    callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().AuthRequiredError));
    return nullptr;
  }

  if (command_name == Common::Redis::SupportedCommands::ping()) {
    // Respond to PING locally.
    Common::Redis::RespValuePtr pong(new Common::Redis::RespValue());
    pong->type(Common::Redis::RespType::SimpleString);
    pong->asString() = "PONG";
    callbacks.onResponse(std::move(pong));
    return nullptr;
  }

  if (command_name == Common::Redis::SupportedCommands::time()) {
    // Respond to TIME locally.
    Common::Redis::RespValuePtr time_resp(new Common::Redis::RespValue());
    time_resp->type(Common::Redis::RespType::Array);
    std::vector<Common::Redis::RespValue> resp_array;

    auto now = dispatcher.timeSource().systemTime().time_since_epoch();

    Common::Redis::RespValue time_in_secs;
    time_in_secs.type(Common::Redis::RespType::BulkString);
    time_in_secs.asString() =
        std::to_string(std::chrono::duration_cast<std::chrono::seconds>(now).count());
    resp_array.push_back(time_in_secs);

    Common::Redis::RespValue time_in_micro_secs;
    time_in_micro_secs.type(Common::Redis::RespType::BulkString);
    time_in_micro_secs.asString() =
        std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(now).count());
    resp_array.push_back(time_in_micro_secs);

    time_resp->asArray().swap(resp_array);
    callbacks.onResponse(std::move(time_resp));
    return nullptr;
  }
  // For transaction type commands and blockingcommands , quit needs to be handled from within the command handler
  if ((command_name == Common::Redis::SupportedCommands::quit() || command_name == Common::Redis::SupportedCommands::exit()) && !callbacks.transaction().active_) {
    callbacks.onQuit();
    return nullptr;
  }

  if (request->asArray().size() < 2 &&(Common::Redis::SupportedCommands::transactionCommands().count(command_name) == 0)
  && (Common::Redis::SupportedCommands::subcrStateallowedCommands().count(command_name) == 0)
  && (Common::Redis::SupportedCommands::noArgCommands().count(command_name) == 0)) {
    // Commands other than PING, TIME and transaction commands all have at least two arguments.
    ENVOY_LOG(debug,"invalid request - not enough arguments for command: '{}'", command_name);
    onInvalidRequest(callbacks);
    return nullptr;
  }

  if (command_name == "client") {
    std::string sub_command = absl::AsciiStrToLower(request->asArray()[1].asString());
    if (Common::Redis::SupportedCommands::clientSubCommands().count(sub_command) == 0) {
      stats_.unsupported_command_.inc();
      ENVOY_LOG(debug, "unsupported command '{}' '{}'",command_name, sub_command);
      callbacks.onResponse(Common::Redis::Utility::makeError(
          fmt::format("unsupported command '{}' '{}'",command_name, sub_command)));
      return nullptr;
    }
    if ((sub_command == "setname" && request->asArray().size() != 3) || (sub_command == "getname" && request->asArray().size() != 2)) {
      callbacks.onResponse(Common::Redis::Utility::makeError(
         fmt::format("ERR wrong number of arguments for CLIENT '{}' command", sub_command)));
      return nullptr;
    }
    Common::Redis::RespValuePtr ClientResp(new Common::Redis::RespValue());
    if (sub_command == "setname") {
      callbacks.setClientname(request->asArray()[2].asString());
      ClientResp->type(Common::Redis::RespType::SimpleString);
      ClientResp->asString() = "OK";
    }else if (sub_command == "getname") {
      ClientResp->type(Common::Redis::RespType::BulkString);
      ClientResp->asString() = callbacks.getClientname();
    }
    callbacks.onResponse(std::move(ClientResp));
    return nullptr;
  }

  // Get the handler for the downstream request
  auto handler = handler_lookup_table_.find(command_name.c_str());
  if (handler == nullptr && !callbacks.transaction().isSubscribedMode()) {
    stats_.unsupported_command_.inc();
    ENVOY_LOG(debug, "unsupported command '{}'", request->asArray()[0].asString());
    callbacks.onResponse(Common::Redis::Utility::makeError(
        fmt::format("unsupported command '{}'", request->asArray()[0].asString())));
    return nullptr;
  }

  // If we are within a transaction, forward all requests to the transaction handler (i.e. handler
  // of "multi" command).
  if (callbacks.transaction().active_ && callbacks.transaction().isTransactionMode()) {
    handler = handler_lookup_table_.find("multi");
  }

  // If we are within a subscribe mode, forward all requests to the pubsub command handler 
  if (callbacks.transaction().active_ &&  callbacks.transaction().isSubscribedMode()) {
    handler = handler_lookup_table_.find("subscribe");
  }

  // Fault Injection Check
  const Common::Redis::Fault* fault_ptr = fault_manager_->getFaultForCommand(command_name);

  // Check if delay, which determines which callbacks to use. If a delay fault is enabled,
  // the delay fault itself wraps the request (or other fault) and the delay fault itself
  // implements the callbacks functions, and in turn calls the real callbacks after injecting
  // delay on the result of the wrapped request or fault.
  const bool has_delay_fault =
      fault_ptr != nullptr && fault_ptr->delayMs() > std::chrono::milliseconds(0);
  std::unique_ptr<DelayFaultRequest> delay_fault_ptr;
  if (has_delay_fault) {
    delay_fault_ptr = DelayFaultRequest::create(callbacks, handler->command_stats_, time_source_,
                                                dispatcher, fault_ptr->delayMs(), stream_info);
  }

  // Note that the command_stats_ object of the original request is used for faults, so that our
  // downstream metrics reflect any faults added (with special fault metrics) or extra latency from
  // a delay. 2) we use a ternary operator for the callback parameter- we want to use the
  // delay_fault as callback if there is a delay per the earlier comment.
  ENVOY_LOG(debug, "splitting '{}'", request->toString());
  handler->command_stats_.total_.inc();

  SplitRequestPtr request_ptr;
  if (fault_ptr != nullptr && fault_ptr->faultType() == Common::Redis::FaultType::Error) {
    request_ptr = ErrorFaultRequest::create(has_delay_fault ? *delay_fault_ptr : callbacks,
                                            handler->command_stats_, time_source_, has_delay_fault,
                                            stream_info);
  } else {
    request_ptr = handler->handler_.get().startRequest(
        std::move(request), has_delay_fault ? *delay_fault_ptr : callbacks, handler->command_stats_,
        time_source_, has_delay_fault, stream_info);
  }

  // Complete delay, if any. The delay fault takes ownership of the wrapped request.
  if (has_delay_fault) {
    delay_fault_ptr->wrapped_request_ptr_ = std::move(request_ptr);
    return delay_fault_ptr;
  } else {
    return request_ptr;
  }
}

void InstanceImpl::onInvalidRequest(SplitCallbacks& callbacks) {
  stats_.invalid_request_.inc();
  callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().InvalidRequest));
}

void InstanceImpl::addHandler(Stats::Scope& scope, const std::string& stat_prefix,
                              const std::string& name, bool latency_in_micros,
                              CommandHandler& handler) {
  std::string to_lower_name = absl::AsciiStrToLower(name);
  const std::string command_stat_prefix = fmt::format("{}command.{}.", stat_prefix, to_lower_name);
  Stats::StatNameManagedStorage storage{command_stat_prefix + std::string("latency"),
                                        scope.symbolTable()};
  handler_lookup_table_.add(
      to_lower_name.c_str(),
      std::make_shared<HandlerData>(HandlerData{
          CommandStats{ALL_COMMAND_STATS(POOL_COUNTER_PREFIX(scope, command_stat_prefix))
                           scope.histogramFromStatName(storage.statName(),
                                                       latency_in_micros
                                                           ? Stats::Histogram::Unit::Microseconds
                                                           : Stats::Histogram::Unit::Milliseconds)},
          handler}));
}

} // namespace CommandSplitter
} // namespace RedisProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

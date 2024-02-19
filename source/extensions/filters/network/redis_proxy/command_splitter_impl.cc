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
 makeScanRequest(const RouteSharedPtr& route, int32_t shard_index, 
                Common::Redis::RespValueConstSharedPtr incoming_request, 
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
  // std::string count = "2";
  std::string Counter = "count";
  std::string key = std::string();
  size_t req_length = 2;
  int32_t shard_idx = 0; 
  int32_t numofRequests= 3;
  std::vector<std::pair<Common::Redis::RespType, std::string>> args;

  Common::Redis::RespValueSharedPtr request = std::make_shared<Common::Redis::RespValue>();
    request->type(Common::Redis::RespType::Array);

  
  // Getting the right cursor before sending request
  // add log for ==4
  std::string cursor = incoming_request->asArray()[1].asString();
  if (cursor.length() > 4) {
    std::string index = cursor.substr(cursor.length() - 4);
    cursor = cursor.substr(0, cursor.length() - 4);
    shard_idx = std::stoi(index);
  }

  Common::Redis::RespValue cstr;
  cstr.type(Common::Redis::RespType::BulkString);
  cstr.asString() = std::move(cursor);

  Common::Redis::RespValue count;
  count.type(Common::Redis::RespType::BulkString);
  count.asString() = std::move("2");

  size_t arraySize = incoming_request->asArray().size();
  for (size_t i = 0; i < arraySize; ++i) {
      std::string arg = incoming_request->asArray()[i].asString();
      Common::Redis::RespValue currentElement;
      currentElement.type(Common::Redis::RespType::BulkString);

      if (i == 1) {
          request->asArray().emplace_back(std::move(cstr));
      } else {
          // Emplace the current argument
          currentElement.asString() = std::move(arg);
          request->asArray().emplace_back(currentElement);
          // request->asArray().emplace_back(std::move(arg));

          // Check if the argument requires a value
          if (arg == "count" || arg == "match" || arg == "type") {
              Common::Redis::RespValue nextElement;
              nextElement.type(Common::Redis::RespType::BulkString);
              nextElement.asString() = std::move(request->asArray()[i + 1].asString()) ;

              // Setting custom count value for "count" argument
              if (arg == "count") {
                  request->asArray().emplace_back(std::move(count));
              } else {
                  // Emplace the value for the argument
                  request->asArray().emplace_back(std::move(nextElement));
              }
              ++i; // Increment i to avoid iterating over the value
              ++arraySize; // Increase array size to accommodate the additional element
          }
      }
  }

  // If count argument is not found, add it with the default value
  if (incoming_request->asArray().size() == 2) {
      Common::Redis::RespValue countElement;
      countElement.type(Common::Redis::RespType::BulkString);
      countElement.asString() = std::move("count");
      request->asArray().emplace_back(countElement);

      Common::Redis::RespValue defaultCountElement;
      defaultCountElement.type(Common::Redis::RespType::BulkString);
      request->asArray().emplace_back(std::move(count));
  }

  std::unique_ptr<ScanRequest> request_ptr{
      new ScanRequest(callbacks, command_stats, time_source, delay_command_latency)};

  request_ptr->request_length_ = req_length;
            
  Common::Redis::RespValueSharedPtr base_request = std::move(incoming_request);
  request_ptr->incoming_request_ = base_request;
  request_ptr->route_ = router.upstreamPool(key, stream_info);
  
  if (request_ptr->route_) {
    Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl* instance =
        dynamic_cast<Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl*>(
            request_ptr->route_->upstream(key).get());
    
    request_ptr->num_of_Shards_ = instance->getNumofRedisShards();
    if (request_ptr->num_of_Shards_ == 0 ) {
      callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
    }
  }
  else{
    callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
  }

  if (shard_idx > request_ptr->num_of_Shards_) {
    shard_idx = 0;
  } else {
    numofRequests = request_ptr->num_of_Shards_ - shard_idx + 1; 
  }

  request_ptr->pending_requests_.reserve(numofRequests);
  request_ptr->pending_responses_.reserve(numofRequests);
  
  for (int32_t i = request_ptr->num_of_Shards_-1; i >= shard_idx ; i--) {
    request_ptr->pending_requests_.emplace_back(*request_ptr, i);
  }
  //keeping shard index and pending_requests_index same

  PendingRequest& pending_request = request_ptr->pending_requests_.back();
  if (request_ptr->route_) {
    pending_request.handle_= makeScanRequest(request_ptr->route_, shard_idx, request, pending_request, callbacks.transaction());
  }

  if (!pending_request.handle_) {
    pending_request.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
    return nullptr;
  }

  return request_ptr;
}

void ScanRequest::onChildResponse(Common::Redis::RespValuePtr&& value, int32_t index) {
  // Request handled successfully
  pending_requests_[index].handle_ = nullptr;
  // Moving the response to pending response for doing validation later
  // Incrementing the number of pending responses, it will drained during the validation
  int64_t count = 2;

  // Checking the cursor and number of objects for child request
  std::string cursor = value->asArray()[0].asString();
  int64_t objects = value->asArray()[1].asArray().size();
  std::string newCount = std::to_string(count - objects);

  if (index >= static_cast<int32_t>(pending_responses_.size())) {
        // Resize the vector to accommodate the new index
        pending_responses_.resize(index + 1);
  }
  ENVOY_LOG(debug,"response recived for index: '{}'", index);

  pending_responses_[index] = std::move(value);
  ++num_pending_responses_;
  bool send_response = true;

  // 1) If cursor is zero, objects less than count
  //  1.1) If No more shards to scan, there won't be any child request
  //  1.2) If shards present, increment the shard index, update the count value, send child request
  // 2) If cursor is not zero
  //  2.1) If objects returned is equal to count, there won't be any child request

  // cache type and match variable

  // if (cursor == "0" && objects < count && index+1 < num_of_Shards_) {
  //   const std::vector<Common::Redis::RespValue> args = Common::Redis::Utility::ArgsGenerator({
  //             {Common::Redis::RespType::BulkString, "0"},
  //             {Common::Redis::RespType::BulkString, "count"},
  //             {Common::Redis::RespType::BulkString, newCount}}).asArray();
  //   const Common::Redis::RespValue get_scan_cmd(
  //           incoming_request_, Common::Redis::Utility::ScanRequest::instance(), args, 1, request_length_-1, "replace");
  //   pending_requests_.pop_back();
  //   PendingRequest& pending_request = pending_requests_.back();
  //   send_response = false;
  //   pending_request.handle_= makeScanRequest(route_, index+1, get_scan_cmd, pending_request, callbacks_.transaction());
  //   if (!pending_request.handle_) {
  //     pending_request.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
  //   }
  // }

  if (send_response) {
    // Cleaning up the stale pending_requests_
    for (auto& request : pending_requests_) {
      request.handle_ = nullptr;
    }
    Common::Redis::RespValuePtr response = std::make_unique<Common::Redis::RespValue>();
    response->type(Common::Redis::RespType::Array);
    
    // Process cursor -> Append cursor with shard index so that next request will come to corresponding shard
    // if cursor is non zero, there are few elements remaining in the shard so set the index to the same shard
    // if cursor is zero, but the count is satisfied, the next request should go to next shard. so increament the index 
    if (cursor != "0" ) {
      std::string indexStr = std::to_string(index);
      while (indexStr.length() < 4) {
          indexStr = "0" + indexStr;
      }
      cursor += indexStr;
    }
    if (cursor == "0" && index+1 < num_of_Shards_) {
      std::string indexStr = std::to_string(index+1);
      while (indexStr.length() < 4) {
          indexStr = "0" + indexStr;
      }
      cursor += indexStr;
    }

    Common::Redis::RespValue cstr;
    cstr.type(Common::Redis::RespType::BulkString);
    cstr.asString() = std::move(cursor);

    // Add the first element of the pending responses to the main response array
    response->asArray().emplace_back(std::move(cstr));

    // Create a temporary array to hold the nested elements
    Common::Redis::RespValue tempArray;
    tempArray.type(Common::Redis::RespType::Array);

    // Iterate through the pending responses, skipping the first one
    for (size_t i = 0; i < pending_responses_.size(); ++i) {
      if (pending_responses_[i] != nullptr) {
        auto& resp = pending_responses_[i];
        if (resp->type() == Common::Redis::RespType::Array) {
            auto& res = resp->asArray()[1];
            if (res.type() == Common::Redis::RespType::Array) {
              // Iterate through the inner array and add its elements to the temporary array, starting from the second element
              for (size_t k = 0; k < res.asArray().size(); ++k) {
                tempArray.asArray().emplace_back(std::move(res.asArray()[k]));
              }
            }
        }
      }
    }
    pending_responses_.clear();

    // Add the temporary array to the main response array as a nested array
    response->asArray().emplace_back(std::move(tempArray));
    if (response->type() == Common::Redis::RespType::Error){
      ENVOY_LOG(debug,"recived error for index: '{}'", index);
      Common::Redis::RespValuePtr response_t = std::move(response);
      ENVOY_LOG(debug, "response: {}", response_t->toString());
      callbacks_.onResponse(std::move(response_t));
    } else {
      updateStats(error_count_ == 0);
      Common::Redis::RespValuePtr response_t = std::move(response);
      ENVOY_LOG(debug, "response: {}", response_t->toString()); 
      callbacks_.onResponse(std::move(response_t));
    }
  }
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


NoKeyAllPrimaryRequest::~NoKeyAllPrimaryRequest() {
#ifndef NDEBUG
  for (const PendingRequest& request : pending_requests_) {
    ASSERT(!request.handle_);
  }
#endif
}

void NoKeyAllPrimaryRequest::cancel() {
  for (PendingRequest& request : pending_requests_) {
    if (request.handle_) {
      request.handle_->cancel();
      request.handle_ = nullptr;
    }
  }
}

void NoKeyAllPrimaryRequest::onChildFailure(int32_t index) {
  onChildResponse(Common::Redis::Utility::makeError(Response::get().UpstreamFailure), index);
}

SplitRequestPtr NoKeyRequest::create(Router& router, Common::Redis::RespValuePtr&& incoming_request,
                                    SplitCallbacks& callbacks, CommandStats& command_stats,
                                    TimeSource& time_source, bool delay_command_latency,
                                    const StreamInfo::StreamInfo& stream_info) {
  std::unique_ptr<NoKeyRequest> request_ptr{
      new NoKeyRequest(callbacks, command_stats, time_source, delay_command_latency)};
  std::string key = std::string();
  const auto& route = router.upstreamPool(key, stream_info);
  if (route) {
    Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl* instance =
        dynamic_cast<Extensions::NetworkFilters::RedisProxy::ConnPool::InstanceImpl*>(
            route->upstream(key).get());
    int32_t numofRedisShards = instance->getNumofRedisShards();
    if (numofRedisShards > 0) {
      request_ptr->num_pending_responses_ = numofRedisShards;
    }
    else{
      ENVOY_LOG(debug, "Upstreams not found: '{}'", incoming_request->toString());
      callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
      //request_ptr->onChildResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost),0);
    }
  }
  else{
    ENVOY_LOG(debug, "route not found: '{}'", incoming_request->toString());
    callbacks.onResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost));
    //request_ptr->onChildResponse(Common::Redis::Utility::makeError(Response::get().NoUpstreamHost),0);
  }
  request_ptr->pending_requests_.reserve(request_ptr->num_pending_responses_);

  // Identify the response type based on the subcommand , needs to be written very clean 
  // along with subcommand identification for each command
  // For now keeping it ugly like this and assuming we only add support for script comand.
  Common::Redis::RespValueSharedPtr base_request = std::move(incoming_request);
  request_ptr->pending_responses_.reserve(request_ptr->num_pending_responses_);
  
  for (int32_t i = 0; i < request_ptr->num_pending_responses_; i++) {
    request_ptr->pending_requests_.emplace_back(*request_ptr, i);
    PendingRequest& pending_request = request_ptr->pending_requests_.back();

    const auto route = router.upstreamPool(key, stream_info);
    if (route) {
      pending_request.handle_= makeNoKeyRequest(route, i,base_request, pending_request, callbacks.transaction());
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

void NoKeyRequest::onChildResponse(Common::Redis::RespValuePtr&& value, int32_t index) {
  pending_requests_[index].handle_ = nullptr;

  if (index >= static_cast<int32_t>(pending_responses_.size())) {
        // Resize the vector to accommodate the new index
        pending_responses_.resize(index + 1);
  }
  ENVOY_LOG(debug,"response recived for index: '{}'", index);

  if (value->type() == Common::Redis::RespType::Error){
    error_count_++;
    response_index_=index;
  }
  // Move the value into the pending_responses at the specified index
  pending_responses_[index] = std::move(value);

  ASSERT(num_pending_responses_ > 0);
  if (--num_pending_responses_ == 0) {
    if (error_count_ > 0 ){
      if (!pending_responses_.empty()) {
        ENVOY_LOG(debug, "Error Response received: '{}'", pending_responses_[response_index_]->toString());
        Common::Redis::RespValuePtr response = std::move(pending_responses_[response_index_]);
        callbacks_.onResponse(std::move(response));
        pending_responses_.clear();
      }
    } else if(! areAllResponsesSame(pending_responses_)) {
      ENVOY_LOG(debug, "all response not same: '{}'", pending_responses_[0]->toString());
      callbacks_.onResponse(Common::Redis::Utility::makeError(
          fmt::format("all responses not same")));
      if (!pending_responses_.empty())    
        pending_responses_.clear();
    }else {
      updateStats(error_count_ == 0);
      if (!pending_responses_.empty()) {
      Common::Redis::RespValuePtr response = std::move(pending_responses_[0]);
      ENVOY_LOG(debug, "response: {}", response->toString()); 
      callbacks_.onResponse(std::move(response));
      pending_responses_.clear();
      }
    }
  }
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

  // Within transactions we only support simple commands.
  // So if this is not a transaction command or a simple command, it is an error.
  if (Common::Redis::SupportedCommands::transactionCommands().count(command_name) == 0 &&
      Common::Redis::SupportedCommands::simpleCommands().count(command_name) == 0) {
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
    // Respond to MULTI locally.
    localResponse(callbacks, "OK");
    return nullptr;

  } else if (command_name == "exec" || command_name == "discard") {
    // Handle the case where we don't have an open transaction.
    if (transaction.active_ == false) {
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
      transaction_handler_(*router_),nokeyrequest_handler_(*router_),scanrequest_handler_(*router_), 
      stats_{ALL_COMMAND_SPLITTER_STATS(POOL_COUNTER_PREFIX(scope, stat_prefix + "splitter."))},
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
  for (const std::string& command : Common::Redis::SupportedCommands::noKeyCommands()) {
    addHandler(scope, stat_prefix, command, latency_in_micros, nokeyrequest_handler_);
  }
}

SplitRequestPtr InstanceImpl::makeRequest(Common::Redis::RespValuePtr&& request,
                                          SplitCallbacks& callbacks, Event::Dispatcher& dispatcher,
                                          const StreamInfo::StreamInfo& stream_info) {
  if ((request->type() != Common::Redis::RespType::Array) || request->asArray().empty()) {
    onInvalidRequest(callbacks);
    return nullptr;
  }

  for (const Common::Redis::RespValue& value : request->asArray()) {
    if (value.type() != Common::Redis::RespType::BulkString) {
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

  if (command_name == Common::Redis::SupportedCommands::quit()) {
    callbacks.onQuit();
    return nullptr;
  }

  if (request->asArray().size() < 2 &&
      Common::Redis::SupportedCommands::transactionCommands().count(command_name) == 0) {
    // Commands other than PING, TIME and transaction commands all have at least two arguments.
    onInvalidRequest(callbacks);
    return nullptr;
  }

  // Get the handler for the downstream request
  auto handler = handler_lookup_table_.find(command_name.c_str());
  if (handler == nullptr) {
    stats_.unsupported_command_.inc();
    callbacks.onResponse(Common::Redis::Utility::makeError(
        fmt::format("unsupported command '{}'", request->asArray()[0].asString())));
    return nullptr;
  }

  // If we are within a transaction, forward all requests to the transaction handler (i.e. handler
  // of "multi" command).
  if (callbacks.transaction().active_) {
    handler = handler_lookup_table_.find("multi");
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

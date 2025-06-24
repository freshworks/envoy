#include "source/extensions/filters/network/redis_proxy/proxy_filter.h"

#include <cstdint>
#include <string>

#include "envoy/extensions/filters/network/redis_proxy/v3/redis_proxy.pb.h"
#include "envoy/stats/scope.h"

#include "source/common/common/assert.h"
#include "source/common/common/fmt.h"
#include "source/common/config/datasource.h"
#include "source/common/config/utility.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace RedisProxy {

ProxyFilterConfig::ProxyFilterConfig(
    const envoy::extensions::filters::network::redis_proxy::v3::RedisProxy& config,
    Stats::Scope& scope, const Network::DrainDecision& drain_decision, Runtime::Loader& runtime,
    Api::Api& api,
    Extensions::Common::DynamicForwardProxy::DnsCacheManagerFactory& cache_manager_factory)
    : drain_decision_(drain_decision), runtime_(runtime),
      stat_prefix_(fmt::format("redis.{}.", config.stat_prefix())),
      stats_(generateStats(stat_prefix_, scope)),
      downstream_auth_username_(
          Config::DataSource::read(config.downstream_auth_username(), true, api)),
      dns_cache_manager_(cache_manager_factory.get()), dns_cache_(getCache(config)) {

  if (config.settings().enable_redirection() && !config.settings().has_dns_cache_config()) {
    ENVOY_LOG(warn, "redirections without DNS lookups enabled might cause client errors, set the "
                    "dns_cache_config field within the connection pool settings to avoid them");
  }

  auto downstream_auth_password =
      Config::DataSource::read(config.downstream_auth_password(), true, api);
  if (!downstream_auth_password.empty()) {
    downstream_auth_passwords_.emplace_back(downstream_auth_password);
  }

  if (config.downstream_auth_passwords_size() > 0) {
    downstream_auth_passwords_.reserve(downstream_auth_passwords_.size() +
                                       config.downstream_auth_passwords().size());
    for (const auto& source : config.downstream_auth_passwords()) {
      const auto p = Config::DataSource::read(source, true, api);
      if (!p.empty()) {
        downstream_auth_passwords_.emplace_back(p);
      }
    }
  }
}

Extensions::Common::DynamicForwardProxy::DnsCacheSharedPtr ProxyFilterConfig::getCache(
    const envoy::extensions::filters::network::redis_proxy::v3::RedisProxy& config) {
  if (config.settings().has_dns_cache_config()) {
    auto cache_or_error = dns_cache_manager_->getCache(config.settings().dns_cache_config());
    if (cache_or_error.status().ok()) {
      return cache_or_error.value();
    }
  }
  return nullptr;
}

ProxyStats ProxyFilterConfig::generateStats(const std::string& prefix, Stats::Scope& scope) {
  return {
      ALL_REDIS_PROXY_STATS(POOL_COUNTER_PREFIX(scope, prefix), POOL_GAUGE_PREFIX(scope, prefix))};
}

ProxyFilter::ProxyFilter(Common::Redis::DecoderFactory& factory,
                         Common::Redis::EncoderPtr&& encoder, CommandSplitter::Instance& splitter,
                         ProxyFilterConfigSharedPtr config)
    : decoder_(factory.create(*this)), encoder_(std::move(encoder)), splitter_(splitter),
      config_(config), transaction_(this) {
  config_->stats_.downstream_cx_total_.inc();
  config_->stats_.downstream_cx_active_.inc();
  connection_allowed_ =
      config_->downstream_auth_username_.empty() && config_->downstream_auth_passwords_.empty();
  connection_quit_ = false;
}

ProxyFilter::~ProxyFilter() {
   ENVOY_LOG(debug,"ProxyFilter Destructor");
  ASSERT(pending_requests_.empty());
  config_->stats_.downstream_cx_active_.dec();
}

void ProxyFilter::initializeReadFilterCallbacks(Network::ReadFilterCallbacks& callbacks) {
  callbacks_ = &callbacks;
  callbacks_->connection().addConnectionCallbacks(*this);
  callbacks_->connection().setConnectionStats({config_->stats_.downstream_cx_rx_bytes_total_,
                                               config_->stats_.downstream_cx_rx_bytes_buffered_,
                                               config_->stats_.downstream_cx_tx_bytes_total_,
                                               config_->stats_.downstream_cx_tx_bytes_buffered_,
                                               nullptr, nullptr});
}

void ProxyFilter::onRespValue(Common::Redis::RespValuePtr&& value) {
  pending_requests_.emplace_back(*this);
  PendingRequest& request = pending_requests_.back();
  CommandSplitter::SplitRequestPtr split =
      splitter_.makeRequest(std::move(value), request, callbacks_->connection().dispatcher(),
                            callbacks_->connection().streamInfo());
  if (split) {
    // The splitter can immediately respond and destroy the pending request. Only store the handle
    // if the request is still alive.
    request.request_handle_ = std::move(split);
  }
}

void ProxyFilter::onEvent(Network::ConnectionEvent event) {
  if (event == Network::ConnectionEvent::Connected) {
    ENVOY_LOG(trace, "new connection to redis proxy filter");
  }
  if (event == Network::ConnectionEvent::RemoteClose ||
      event == Network::ConnectionEvent::LocalClose) {
    ENVOY_LOG(debug, "connection to redis proxy filter closed: {}", event == Network::ConnectionEvent::RemoteClose ? " remotely" : "locally");
    while (!pending_requests_.empty()) {
      if (pending_requests_.front().request_handle_ != nullptr) {
        pending_requests_.front().request_handle_->cancel();
      }else{
        ENVOY_LOG(error,"pending request handle is null");
      }
      pending_requests_.pop_front();
      request_cancelled_=true;
    }
    if (event == Network::ConnectionEvent::RemoteClose){
       ENVOY_LOG(debug,"dereferencing pubsub callback and transaction on exit from proxy filter");
      // As downstreamcallbaks is created in proxy filter irerespecive of its a pubsub command or not this needs to be cleared on exit from proxy filter
      // decrement the reference to proxy filter
      auto downstream_cb = dynamic_cast<DownStreamCallbacks*>(transaction_.getDownstreamCallback().get());
      if (downstream_cb != nullptr){
        downstream_cb->clearParent();
      }
      transaction_.setDownstreamCallback(nullptr);
      if(transaction_.isSubscribedMode()){
        ENVOY_LOG(debug,"ProxyFilter::onEvent Clearing pubsubcb");
        transaction_.setPubSubCallback(nullptr);

      }
      if (transaction_.isTransactionMode()){
        this->closeDownstreamConnection();
      }

    }
    ENVOY_LOG(debug,"closing downstream connection with transaction_.close()");
    transaction_.close();
  }
}

void ProxyFilter::onQuit(PendingRequest& request) {
  Common::Redis::RespValuePtr response{new Common::Redis::RespValue()};
  response->type(Common::Redis::RespType::SimpleString);
  response->asString() = "OK";
  connection_quit_ = true;
  request.onResponse(std::move(response));
}

void ProxyFilter::onAuth(PendingRequest& request, const std::string& password) {
  Common::Redis::RespValuePtr response{new Common::Redis::RespValue()};
  if (config_->downstream_auth_passwords_.empty()) {
    response->type(Common::Redis::RespType::Error);
    response->asString() = "ERR Client sent AUTH, but no password is set";
  } else if (checkPassword(password)) {
    response->type(Common::Redis::RespType::SimpleString);
    response->asString() = "OK";
    connection_allowed_ = true;
  } else {
    response->type(Common::Redis::RespType::Error);
    response->asString() = "ERR invalid password";
    connection_allowed_ = false;
  }
  request.onResponse(std::move(response));
}

void ProxyFilter::onAuth(PendingRequest& request, const std::string& username,
                         const std::string& password) {
  Common::Redis::RespValuePtr response{new Common::Redis::RespValue()};
  if (config_->downstream_auth_username_.empty() && config_->downstream_auth_passwords_.empty()) {
    response->type(Common::Redis::RespType::Error);
    response->asString() = "ERR Client sent AUTH, but no username-password pair is set";
  } else if (config_->downstream_auth_username_.empty() && username == "default" &&
             checkPassword(password)) {
    // empty username and "default" are synonymous in Redis 6 ACLs
    response->type(Common::Redis::RespType::SimpleString);
    response->asString() = "OK";
    connection_allowed_ = true;
  } else if (username == config_->downstream_auth_username_ && checkPassword(password)) {
    response->type(Common::Redis::RespType::SimpleString);
    response->asString() = "OK";
    connection_allowed_ = true;
  } else {
    response->type(Common::Redis::RespType::Error);
    response->asString() = "WRONGPASS invalid username-password pair";
    connection_allowed_ = false;
  }
  request.onResponse(std::move(response));
}

bool ProxyFilter::checkPassword(const std::string& password) {
  for (const auto& p : config_->downstream_auth_passwords_) {
    if (password == p) {
      return true;
    }
  }
  return false;
}
void ProxyFilter::onAsyncResponse(Common::Redis::RespValuePtr&& value){
  encoder_->encode(*value, encoder_buffer_);

  if (encoder_buffer_.length() > 0) {
    callbacks_->connection().write(encoder_buffer_, false);
  }

  // Check if there is an active transaction that needs to be closed.
  if (transaction_.should_close_ || transaction_.is_blocking_command_) {
      //Close all upsteam clients and ref to pubsub callbacks if any
      transaction_.close();
  }
  //Need fix for drain close when pubsub chanels are active  -- to be done
  if(config_->drain_decision_.drainClose() &&
      config_->runtime_.snapshot().featureEnabled(config_->redis_drain_close_runtime_key_, 100)) {
    config_->stats_.downstream_cx_drain_close_.inc();
    //callbacks_->connection().close(Network::ConnectionCloseType::FlushWrite);
    this->closeDownstreamConnection();
  }
  
}
void ProxyFilter::closeDownstreamConnection() {

   ENVOY_LOG(debug,"dereferencing pubsub callback and transaction on exit from proxy filter");
      // As downstreamcallbaks is created in proxy filter irerespecive of its a pubsub command or not this needs to be cleared on exit from proxy filter
      // decrement the reference to proxy filter
      auto downstream_cb = dynamic_cast<DownStreamCallbacks*>(transaction_.getDownstreamCallback().get());
      if (downstream_cb != nullptr){
        downstream_cb->clearParent();
      }
      transaction_.setDownstreamCallback(nullptr);
  callbacks_->connection().close(Network::ConnectionCloseType::FlushWrite);

}
void ProxyFilter::onPubsubConnClose(){
  ASSERT(pending_requests_.empty());
//Close the downstream connection on upstream connection close 
  transaction_.setPubSubCallback(nullptr);
  //callbacks_->connection().close(Network::ConnectionCloseType::FlushWrite);
  //This callback is called only on remote close , so no need to close the client connnection again
  transaction_.connection_established_=false;
  connection_quit_ = false;
  this->closeDownstreamConnection();
  return;
}

void ProxyFilter::onResponse(PendingRequest& request, Common::Redis::RespValuePtr&& value) {
  //Commenting this out for Trackign an issue in freshchat where the pending_requests_ is empty but we get a response
  //ASSERT(!pending_requests_.empty());
  if (pending_requests_.empty()) {
    ENVOY_LOG(error, "Received response:'{}' with no pending requests - discarding response",value->toString());
    ENVOY_LOG(error, "cancel request '{}' when no pending requests",request_cancelled_?"true":"false");
    if (request.request_handle_ != nullptr) {
      request.request_handle_=nullptr;
    }
    return;
  }
  request.pending_response_ = std::move(value);
  request.request_handle_ = nullptr;

  if (request.pending_response_.get()->type() == Common::Redis::RespType::Null && transaction_.isSubscribedMode()){
    ENVOY_LOG(debug,"Null response received from upstream Possible pubsub message processing, ignoring sending response downstream");
    request.pending_response_.reset();
    pending_requests_.pop_front();
    return;
  }

  if (request.pending_response_.get()->type() != Common::Redis::RespType::Null && transaction_.isSubscribedMode()){
    ENVOY_LOG(error,"Non null response received for pubsub command: : '{}' ",request.pending_response_->toString());
  }

  if (request.pending_response_) {
    if (request.pending_response_->type() != Common::Redis::RespType::Null &&request.pending_response_->type() == Common::Redis::RespType::Error) {
      ENVOY_LOG(info, "error response: '{}'", request.pending_response_->toString());
    }
  }
  // The response we got might not be in order, so flush out what we can. (A new response may
  // unlock several out of order responses).
  while (!pending_requests_.empty() && pending_requests_.front().pending_response_) {
    encoder_->encode(*pending_requests_.front().pending_response_, encoder_buffer_);
    pending_requests_.pop_front();
  }

  if (encoder_buffer_.length() > 0) {
    callbacks_->connection().write(encoder_buffer_, false);
  }
  if (pending_requests_.empty() && connection_quit_) {
    ENVOY_LOG(debug,"closing downstream connection as no pending requests and connection quit");
    //callbacks_->connection().close(Network::ConnectionCloseType::FlushWrite);
    this->closeDownstreamConnection();
    connection_quit_ = false;
    return;
  }

  // Check for drain close only if there are no pending responses.
  if (pending_requests_.empty() && config_->drain_decision_.drainClose() &&
      config_->runtime_.snapshot().featureEnabled(config_->redis_drain_close_runtime_key_, 100)) {
    config_->stats_.downstream_cx_drain_close_.inc();
    //callbacks_->connection().close(Network::ConnectionCloseType::FlushWrite);
    ENVOY_LOG(debug,"closing downstream connection as no pending requests and drain close");
    this->closeDownstreamConnection();
  }

  // Check if there is an active transaction that needs to be closed.
  if ((transaction_.should_close_ && pending_requests_.empty()) || 
      (transaction_.isBlockingCommand() && pending_requests_.empty()) ||
      (transaction_.isSubscribedMode() && transaction_.should_close_)) {
    if (transaction_.should_close_ == true && transaction_.isBlockingCommand()) {
      //callbacks_->connection().close(Network::ConnectionCloseType::FlushWrite);
      ENVOY_LOG(debug,"closing downstream connection as blocking command and transaction close");
      this->closeDownstreamConnection();
    }
    ENVOY_LOG(debug,"closing transaction as no pending requests and transaction close");
    transaction_.close();
    //Not sure if for transaction mode also we need to close the connection in downstream
    if (transaction_.isSubscribedMode()){
      transaction_.subscribed_client_shard_index_ = -1;
      //callbacks_->connection().close(Network::ConnectionCloseType::FlushWrite);
      ENVOY_LOG(debug,"closing downstream connection as pubsub mode and transaction close");
      this->closeDownstreamConnection();
    }
    connection_quit_ = false;
    return;
  }
}

Network::FilterStatus ProxyFilter::onData(Buffer::Instance& data, bool) {
  TRY_NEEDS_AUDIT {
    decoder_->decode(data);
    return Network::FilterStatus::Continue;
  }
  END_TRY catch (Common::Redis::ProtocolError&) {
    config_->stats_.downstream_cx_protocol_error_.inc();
    Common::Redis::RespValue error;
    error.type(Common::Redis::RespType::Error);
    error.asString() = "downstream protocol error";
    encoder_->encode(error, encoder_buffer_);
    callbacks_->connection().write(encoder_buffer_, false);
    callbacks_->connection().close(Network::ConnectionCloseType::NoFlush);
    return Network::FilterStatus::StopIteration;
  }
}

ProxyFilter::PendingRequest::PendingRequest(ProxyFilter& parent) : parent_(parent) {
  parent.config_->stats_.downstream_rq_total_.inc();
  parent.config_->stats_.downstream_rq_active_.inc();
}

ProxyFilter::PendingRequest::~PendingRequest() {
  parent_.config_->stats_.downstream_rq_active_.dec();
}

std::unique_ptr<Common::Redis::Utility::DownStreamMetrics>  ProxyFilter::getDownStreamInfo() {
    std::unique_ptr<Common::Redis::Utility::DownStreamMetrics> downstream_metrics = std::make_unique<Common::Redis::Utility::DownStreamMetrics>();
    downstream_metrics->downstream_rq_total_ = config_->stats_.downstream_rq_total_.value();
    downstream_metrics->downstream_cx_drain_close_ = config_->stats_.downstream_cx_drain_close_.value();
    downstream_metrics->downstream_cx_protocol_error_ = config_->stats_.downstream_cx_protocol_error_.value();
    downstream_metrics->downstream_cx_rx_bytes_total_ = config_->stats_.downstream_cx_rx_bytes_total_.value();
    downstream_metrics->downstream_cx_total_ = config_->stats_.downstream_cx_total_.value();
    downstream_metrics->downstream_cx_tx_bytes_total_ = config_->stats_.downstream_cx_tx_bytes_total_.value();
    downstream_metrics->downstream_cx_active_ = config_->stats_.downstream_cx_active_.value();
    downstream_metrics->downstream_cx_rx_bytes_buffered_ = config_->stats_.downstream_cx_rx_bytes_buffered_.value();
    downstream_metrics->downstream_cx_tx_bytes_buffered_ = config_->stats_.downstream_cx_tx_bytes_buffered_.value();
    downstream_metrics->downstream_rq_active_ = config_->stats_.downstream_rq_active_.value();

    //Fill the connection and soccket related metrics
    downstream_metrics->local_address_ = callbacks_->connection().connectionInfoProvider().localAddress()->asString();
    downstream_metrics->remote_address_ = callbacks_->connection().connectionInfoProvider().remoteAddress()->asString();
    if (callbacks_->connection().connectionInfoProvider().connectionID().has_value()){
      downstream_metrics->connection_id_ = callbacks_->connection().connectionInfoProvider().connectionID().value();
    }
    else{
      downstream_metrics->connection_id_ = 0;
    }
    return downstream_metrics;
  }


} // namespace RedisProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

#include "source/extensions/filters/network/common/redis/client_impl.h"

#include "envoy/extensions/filters/network/redis_proxy/v3/redis_proxy.pb.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace Common {
namespace Redis {
namespace Client {
namespace {
// null_pool_callbacks is used for requests that must be filtered and not redirected such as
// "asking".
Common::Redis::Client::DoNothingPoolCallbacks null_pool_callbacks;
} // namespace

ConfigImpl::ConfigImpl(
    const envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings&
        config)
    : op_timeout_(PROTOBUF_GET_MS_REQUIRED(config, op_timeout)),
      enable_hashtagging_(config.enable_hashtagging()),
      enable_redirection_(config.enable_redirection()),
      max_buffer_size_before_flush_(
          config.max_buffer_size_before_flush()), // This is a scalar, so default is zero.
      buffer_flush_timeout_(PROTOBUF_GET_MS_OR_DEFAULT(
          config, buffer_flush_timeout,
          3)), // Default timeout is 3ms. If max_buffer_size_before_flush is zero, this is not used
               // as the buffer is flushed on each request immediately.
      max_upstream_unknown_connections_(
          PROTOBUF_GET_WRAPPED_OR_DEFAULT(config, max_upstream_unknown_connections, 100)),
      enable_command_stats_(config.enable_command_stats()) {
  switch (config.read_policy()) {
    PANIC_ON_PROTO_ENUM_SENTINEL_VALUES;
  case envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings::MASTER:
    read_policy_ = ReadPolicy::Primary;
    break;
  case envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings::
      PREFER_MASTER:
    read_policy_ = ReadPolicy::PreferPrimary;
    break;
  case envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings::REPLICA:
    read_policy_ = ReadPolicy::Replica;
    break;
  case envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings::
      PREFER_REPLICA:
    read_policy_ = ReadPolicy::PreferReplica;
    break;
  case envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings::ANY:
    read_policy_ = ReadPolicy::Any;
    break;
  }

  if (config.has_connection_rate_limit()) {
    connection_rate_limit_enabled_ = true;
    connection_rate_limit_per_sec_ = config.connection_rate_limit().connection_rate_limit_per_sec();
  } else {
    connection_rate_limit_enabled_ = false;
    connection_rate_limit_per_sec_ = 100;
  }
}

ClientPtr ClientImpl::create(Upstream::HostConstSharedPtr host, Event::Dispatcher& dispatcher,
                             EncoderPtr&& encoder, DecoderFactory& decoder_factory,
                             const Config& config,
                             const RedisCommandStatsSharedPtr& redis_command_stats,
                             Stats::Scope& scope, bool is_transaction_client, bool is_pubsub_client, bool is_blocking_client, const std::shared_ptr<PubsubCallbacks>& pubsubcb) {
  auto client =
      std::make_unique<ClientImpl>(host, dispatcher, std::move(encoder), decoder_factory, config,
                                   redis_command_stats, scope, is_transaction_client,is_pubsub_client,is_blocking_client,pubsubcb);
  client->connection_ = host->createConnection(dispatcher, nullptr, nullptr).connection_;
  client->connection_->addConnectionCallbacks(*client);
  client->connection_->addReadFilter(Network::ReadFilterSharedPtr{new UpstreamReadFilter(*client)});
  client->connection_->connect();
  client->connection_->noDelay(true);
  return client;
}

ClientImpl::ClientImpl(Upstream::HostConstSharedPtr host, Event::Dispatcher& dispatcher,
                       EncoderPtr&& encoder, DecoderFactory& decoder_factory, const Config& config,
                       const RedisCommandStatsSharedPtr& redis_command_stats, Stats::Scope& scope,
                       bool is_transaction_client, bool is_pubsub_client, bool is_blocking_client, const std::shared_ptr<PubsubCallbacks>& pubsubcb)
    : host_(host), encoder_(std::move(encoder)), decoder_(decoder_factory.create(*this)),
      config_(config),
      connect_or_op_timer_(dispatcher.createTimer([this]() { onConnectOrOpTimeout(); })),
      flush_timer_(dispatcher.createTimer([this]() { flushBufferAndResetTimer(); })),
      time_source_(dispatcher.timeSource()), redis_command_stats_(redis_command_stats),
      scope_(scope), is_transaction_client_(is_transaction_client),  is_pubsub_client_(is_pubsub_client), is_blocking_client_(is_blocking_client) {
  Upstream::ClusterTrafficStats& traffic_stats = *host->cluster().trafficStats();
  traffic_stats.upstream_cx_total_.inc();
  host->stats().cx_total_.inc();
  traffic_stats.upstream_cx_active_.inc();
  host->stats().cx_active_.inc();
  connect_or_op_timer_->enableTimer(host->cluster().connectTimeout());
  if (is_pubsub_client_){
    pubsub_cb_ = std::move(pubsubcb);
  }
}

ClientImpl::~ClientImpl() {
  ASSERT(pending_requests_.empty());
  ASSERT(connection_->state() == Network::Connection::State::Closed);
  host_->cluster().trafficStats()->upstream_cx_active_.dec();
  host_->stats().cx_active_.dec();
  pubsub_cb_.reset();

}

void ClientImpl::close() {
  pubsub_cb_.reset();
  if (connection_) {
    connection_->close(Network::ConnectionCloseType::NoFlush); 
  }
}

void ClientImpl::flushBufferAndResetTimer() {
  if (flush_timer_->enabled()) {
    flush_timer_->disableTimer();
  }
  connection_->write(encoder_buffer_, false);
}

PoolRequest* ClientImpl::makeRequest(const RespValue& request, ClientCallbacks& callbacks) {
  ASSERT(connection_->state() == Network::Connection::State::Open);

  const bool empty_buffer = encoder_buffer_.length() == 0;

  Stats::StatName command;
  if (config_.enableCommandStats()) {
    // Only lowercase command and get StatName if we enable command stats
    command = redis_command_stats_->getCommandFromRequest(request);
    redis_command_stats_->updateStatsTotal(scope_, command);
  } else {
    // If disabled, we use a placeholder stat name "unused" that is not used
    command = redis_command_stats_->getUnusedStatName();
  }

  pending_requests_.emplace_back(*this, callbacks, command);
  encoder_->encode(request, encoder_buffer_);

  // If buffer is full, flush. If the buffer was empty before the request, start the timer.
  if (encoder_buffer_.length() >= config_.maxBufferSizeBeforeFlush()) {
    flushBufferAndResetTimer();
  } else if (empty_buffer) {
    flush_timer_->enableTimer(std::chrono::milliseconds(config_.bufferFlushTimeoutInMs()));
  }

  // Only boost the op timeout if:
  // - We are not already connected. Otherwise, we are governed by the connect timeout and the timer
  //   will be reset when/if connection occurs. This allows a relatively long connection spin up
  //   time for example if TLS is being used.
  // - This is the first request on the pipeline. Otherwise the timeout would effectively start on
  //   the last operation.
  if (connected_ && pending_requests_.size() == 1) {
    if (!is_blocking_client_) {
      connect_or_op_timer_->enableTimer(config_.opTimeout());
    }
  }

  return &pending_requests_.back();
}


bool ClientImpl::makePubSubRequest(const RespValue& request) {
  ASSERT(connection_->state() == Network::Connection::State::Open);

  const bool empty_buffer = encoder_buffer_.length() == 0;

  Stats::StatName command;
  if (config_.enableCommandStats()) {
    // Only lowercase command and get StatName if we enable command stats
    command = redis_command_stats_->getCommandFromRequest(request);
    redis_command_stats_->updateStatsTotal(scope_, command);
  } else {
    // If disabled, we use a placeholder stat name "unused" that is not used
    command = redis_command_stats_->getUnusedStatName();
  }

  encoder_->encode(request, encoder_buffer_);

  // If buffer is full, flush. If the buffer was empty before the request, start the timer.
  if (encoder_buffer_.length() >= config_.maxBufferSizeBeforeFlush()) {
    flushBufferAndResetTimer();
  } else if (empty_buffer) {
    flush_timer_->enableTimer(std::chrono::milliseconds(config_.bufferFlushTimeoutInMs()));
  }

  // Only boost the op timeout if:
  // - We are  already connected. Otherwise, we are governed by the connect timeout and the timer
  //   will be reset when/if connection occurs. This allows a relatively long connection spin up
  //   time for example if TLS is being used.
  // - Timer is not already armed. Otherwise the timeout would effectively start on
  if (connected_ && !connect_or_op_timer_->enabled()){
      connect_or_op_timer_->enableTimer(config_.opTimeout());
  }

  return true;
}


void ClientImpl::onConnectOrOpTimeout() {
  putOutlierEvent(Upstream::Outlier::Result::LocalOriginTimeout);
  if (connected_) {
    host_->cluster().trafficStats()->upstream_rq_timeout_.inc();
    host_->stats().rq_timeout_.inc();
  } else {
    host_->cluster().trafficStats()->upstream_cx_connect_timeout_.inc();
    host_->stats().cx_connect_fail_.inc();
  }

  if (!is_blocking_client_) {
    connection_->close(Network::ConnectionCloseType::NoFlush);
  } else {
    ENVOY_LOG(debug, "Ignoring timeout for connection close for Blocking clients!");
  }
}

void ClientImpl::onData(Buffer::Instance& data) {
  TRY_NEEDS_AUDIT { decoder_->decode(data); }
  END_TRY catch (ProtocolError&) {
    putOutlierEvent(Upstream::Outlier::Result::ExtOriginRequestFailed);
    host_->cluster().trafficStats()->upstream_cx_protocol_error_.inc();
    host_->stats().rq_error_.inc();
    connection_->close(Network::ConnectionCloseType::NoFlush);
  }
}

void ClientImpl::putOutlierEvent(Upstream::Outlier::Result result) {
  if (!config_.disableOutlierEvents()) {
    host_->outlierDetector().putResult(result);
  }
}

void ClientImpl::onEvent(Network::ConnectionEvent event) {
  if (event == Network::ConnectionEvent::RemoteClose ||
      event == Network::ConnectionEvent::LocalClose) {

    std::string eventTypeStr = (event == Network::ConnectionEvent::RemoteClose ? "RemoteClose" : "LocalClose");
    ENVOY_LOG(info,"Upstream Client Connection close event received:'{}'",eventTypeStr);
    Upstream::reportUpstreamCxDestroy(host_, event);
    if (!pending_requests_.empty()) {
      Upstream::reportUpstreamCxDestroyActiveRequest(host_, event);
      if (event == Network::ConnectionEvent::RemoteClose) {
        putOutlierEvent(Upstream::Outlier::Result::LocalOriginConnectFailed);
      }
    }
    // If client is Pubsub handle the upstream close event such that downstream must also be closed.
    if ( is_pubsub_client_) {
      host_->cluster().trafficStats()->upstream_cx_destroy_with_active_rq_.inc();
      ENVOY_LOG(info,"Pubsub Client Connection close event received:'{}', clearing pubsub_cb_",eventTypeStr);
      //clear pubsub_cb_ be it either local or remote close 
      pubsub_cb_.reset();

        if ((pubsub_cb_ != nullptr)&&(event == Network::ConnectionEvent::RemoteClose)){
          ENVOY_LOG(info,"Pubsub Client Remote close received on Downstream Notify Upstream and close it");
          pubsub_cb_->onFailure();
        }
    }

    // if (is_transaction_client_) {
    //   ENVOY_LOG(debug, "transaction client {}", is_transaction_client_);
    //   //TODO - Handle transaction client upstream client failures here..
    // }

    //handle non blocking and non transaction requests
    while (!pending_requests_.empty()) { 
      PendingRequest& request = pending_requests_.front();
      if (!request.canceled_) {
        request.callbacks_.onFailure();
      } else {
        host_->cluster().trafficStats()->upstream_rq_cancelled_.inc();
      }
      pending_requests_.pop_front();
    }

    connect_or_op_timer_->disableTimer();
    
  } else if (event == Network::ConnectionEvent::Connected) {
    connected_ = true;
    if (!is_pubsub_client_){
      ASSERT(!pending_requests_.empty());
    }else{
      ENVOY_LOG(info,"Pubsub Client Connection established");
    }
    if (!is_blocking_client_) {
      connect_or_op_timer_->enableTimer(config_.opTimeout());
    }
  }

  if (event == Network::ConnectionEvent::RemoteClose && !connected_) {
    host_->cluster().trafficStats()->upstream_cx_connect_fail_.inc();
    host_->stats().cx_connect_fail_.inc();
  }
}

void ClientImpl::onRespValue(RespValuePtr&& value) {
  int32_t clientIndex = getCurrentClientIndex();
  if (is_pubsub_client_) {
    // This is a pubsub client, and we have received a message from the server.
    // We need to pass this message to the registered callback.
    if (pubsub_cb_ != nullptr){
        pubsub_cb_->handleChannelMessage(std::move(value), clientIndex);
        if (connect_or_op_timer_->enabled()){
          connect_or_op_timer_->disableTimer();
        }
    }else {
      ENVOY_LOG(debug,"Pubsub Client Received message from server but no callback registered");
    }
    return;
  }
  ASSERT(!pending_requests_.empty());
  PendingRequest& request = pending_requests_.front();
  const bool canceled = request.canceled_;

  if (config_.enableCommandStats()) {
    bool success = !canceled && (value->type() != Common::Redis::RespType::Error);
    redis_command_stats_->updateStats(scope_, request.command_, success);
    request.command_request_timer_->complete();
  }
  request.aggregate_request_timer_->complete();

  ClientCallbacks& callbacks = request.callbacks_;

  // We need to ensure the request is popped before calling the callback, since the callback might
  // result in closing the connection.
  pending_requests_.pop_front();
  if (canceled) {
    host_->cluster().trafficStats()->upstream_rq_cancelled_.inc();
  } else if (config_.enableRedirection() && (!is_blocking_client_ || !is_transaction_client_) &&
             (value->type() == Common::Redis::RespType::Error)) {
    std::vector<absl::string_view> err = StringUtil::splitToken(value->asString(), " ", false);
    if (err.size() == 3 &&
        (err[0] == RedirectionResponse::get().MOVED || err[0] == RedirectionResponse::get().ASK)) {
      // MOVED and ASK redirection errors have the following substrings: MOVED or ASK (err[0]), hash
      // key slot (err[1]), and IP address and TCP port separated by a colon (err[2])
      callbacks.onRedirection(std::move(value), std::string(err[2]),
                              err[0] == RedirectionResponse::get().ASK);
    } else {
      if (err[0] == RedirectionResponse::get().CLUSTER_DOWN) {
        callbacks.onFailure();
      } else {
        callbacks.onResponse(std::move(value));
      }
    }
  } else {
    callbacks.onResponse(std::move(value));
  }

  // If there are no remaining ops in the pipeline we need to disable the timer.
  // Otherwise we boost the timer since we are receiving responses and there are more to flush
  // out.
  if (pending_requests_.empty()) {
    connect_or_op_timer_->disableTimer();
  } else {
    connect_or_op_timer_->enableTimer(config_.opTimeout());
  }

  putOutlierEvent(Upstream::Outlier::Result::ExtOriginRequestSuccess);
}

ClientImpl::PendingRequest::PendingRequest(ClientImpl& parent, ClientCallbacks& callbacks,
                                           Stats::StatName command)
    : parent_(parent), callbacks_(callbacks), command_{command},
      aggregate_request_timer_(parent_.redis_command_stats_->createAggregateTimer(
          parent_.scope_, parent_.time_source_)) {
  if (parent_.config_.enableCommandStats()) {
    command_request_timer_ = parent_.redis_command_stats_->createCommandTimer(
        parent_.scope_, command_, parent_.time_source_);
  }
  parent.host_->cluster().trafficStats()->upstream_rq_total_.inc();
  parent.host_->stats().rq_total_.inc();
  parent.host_->cluster().trafficStats()->upstream_rq_active_.inc();
  parent.host_->stats().rq_active_.inc();
}

ClientImpl::PendingRequest::~PendingRequest() {
  parent_.host_->cluster().trafficStats()->upstream_rq_active_.dec();
  parent_.host_->stats().rq_active_.dec();
}

void ClientImpl::PendingRequest::cancel() {
  // If we get a cancellation, we just mark the pending request as cancelled, and then we drop
  // the response as it comes through. There is no reason to blow away the connection when the
  // remote is already responding as fast as possible.
  canceled_ = true;
}

void ClientImpl::initialize(const std::string& auth_username, const std::string& auth_password) {
  if (!auth_username.empty()) {
    // Send an AUTH command to the upstream server with username and password.
    Utility::AuthRequest auth_request(auth_username, auth_password);
    makeRequest(auth_request, null_pool_callbacks);
  } else if (!auth_password.empty()) {
    // Send an AUTH command to the upstream server.
    Utility::AuthRequest auth_request(auth_password);
    makeRequest(auth_request, null_pool_callbacks);
  }
  // Any connection to replica requires the READONLY command in order to perform read.
  // Also the READONLY command is a no-opt for the primary.
  // We only need to send the READONLY command iff it's possible that the host is a replica.
  if (config_.readPolicy() != Common::Redis::Client::ReadPolicy::Primary) {
    makeRequest(Utility::ReadOnlyRequest::instance(), null_pool_callbacks);
  }
}

ClientFactoryImpl ClientFactoryImpl::instance_;

ClientPtr ClientFactoryImpl::create(Upstream::HostConstSharedPtr host,
                                    Event::Dispatcher& dispatcher, const Config& config,
                                    const RedisCommandStatsSharedPtr& redis_command_stats,
                                    Stats::Scope& scope, const std::string& auth_username,
                                    const std::string& auth_password, bool is_transaction_client, bool is_pubsub_client,bool is_blocking_client, const std::shared_ptr<PubsubCallbacks>& pubsubcb) {
  ClientPtr client =
      ClientImpl::create(host, dispatcher, EncoderPtr{new EncoderImpl()}, decoder_factory_, config,
                         redis_command_stats, scope, is_transaction_client, is_pubsub_client,is_blocking_client, pubsubcb);
  client->initialize(auth_username, auth_password);
  return client;
}

} // namespace Client
} // namespace Redis
} // namespace Common
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

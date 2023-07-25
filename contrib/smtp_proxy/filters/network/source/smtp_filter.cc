#include "contrib/smtp_proxy/filters/network/source/smtp_filter.h"

#include <iostream>
#include <string>

#include "envoy/buffer/buffer.h"
#include "envoy/network/connection.h"

#include "source/common/common/assert.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace SmtpProxy {

SmtpFilterConfig::SmtpFilterConfig(const SmtpFilterConfigOptions& config_options,
                                   Stats::Scope& scope)
    : scope_{scope}, stats_(generateStats(config_options.stats_prefix_, scope)),
      tracing_(config_options.tracing_), upstream_tls_(config_options.upstream_tls_) {
  access_logs_ = config_options.access_logs_;
}

SmtpFilter::SmtpFilter(SmtpFilterConfigSharedPtr config, TimeSource& time_source,
                       Random::RandomGenerator& random_generator)
    : config_{config}, time_source_(time_source), random_generator_(random_generator) {}

void SmtpFilter::emitLogEntry(StreamInfo::StreamInfo& stream_info) {
  for (const auto& access_log : config_->accessLogs()) {
    access_log->log(nullptr, nullptr, nullptr, stream_info);
  }
}

Network::FilterStatus SmtpFilter::onNewConnection() {
  incSmtpSessionRequests();
  if (!decoder_) {
    decoder_ = createDecoder(this, time_source_, random_generator_);
  }
  return Network::FilterStatus::Continue;
}

bool SmtpFilter::downstreamStartTls(absl::string_view response) {
  Buffer::OwnedImpl buffer;
  buffer.add(response);

  read_callbacks_->connection().addBytesSentCallback([=](uint64_t bytes) -> bool {
    // Wait until response has been sent.
    if (bytes >= response.length()) {
      if (!read_callbacks_->connection().startSecureTransport()) {
        ENVOY_CONN_LOG(trace, "smtp_proxy filter: cannot switch to tls",
                       read_callbacks_->connection(), bytes);
        return true;
      } else {
        // Switch to TLS has been completed.
        ENVOY_CONN_LOG(trace, "smtp_proxy filter: switched to tls", read_callbacks_->connection(),
                       bytes);
        return false;
      }
    }
    return true;
  });

  read_callbacks_->connection().write(buffer, false);
  return false;
}

bool SmtpFilter::sendUpstream(Buffer::Instance& data) {
  read_callbacks_->injectReadDataToFilterChain(data, false);
  return true;
}

bool SmtpFilter::sendReplyDownstream(absl::string_view response) {
  Buffer::OwnedImpl buffer;
  buffer.add(response);
  if (read_callbacks_->connection().state() != Network::Connection::State::Open) {
    ENVOY_LOG(warn, "downstream connection is closed or closing");
    return true;
  }
  read_callbacks_->connection().addBytesSentCallback([=](uint64_t bytes) -> bool {
    // Wait until response has been sent.
    if (bytes >= response.length()) {
      return false;
    }
    return true;
  });

  read_callbacks_->connection().write(buffer, false);
  return false;
}

bool SmtpFilter::upstreamTlsRequired() const {
  return (config_->upstream_tls_ ==
          envoy::extensions::filters::network::smtp_proxy::v3alpha::SmtpProxy::REQUIRE);
}

bool SmtpFilter::tracingEnabled() { return config_->tracing_; }

bool SmtpFilter::upstreamStartTls() {
  // Try to switch upstream connection to use a secure channel.
  if (read_callbacks_->startUpstreamSecureTransport()) {
    config_->stats_.smtp_session_upstream_tls_success_.inc();
    ENVOY_CONN_LOG(trace, "smtp_proxy: upstream TLS enabled.", read_callbacks_->connection());
  } else {
    ENVOY_CONN_LOG(info,
                   "smtp_proxy: cannot enable upstream secure transport. Check "
                   "configuration. Terminating.",
                   read_callbacks_->connection());
    config_->stats_.smtp_session_upstream_tls_failed_.inc();
    return false;
  }
  return true;
}

void SmtpFilter::incSmtpTransactionRequests() { config_->stats_.smtp_transaction_req_.inc(); }
void SmtpFilter::incSmtpTransactionsCompleted() {
  config_->stats_.smtp_transaction_completed_.inc();
}

void SmtpFilter::incSmtpTransactionsAborted() { config_->stats_.smtp_transaction_aborted_.inc(); }
void SmtpFilter::incSmtpSessionRequests() { config_->stats_.smtp_session_requests_.inc(); }
void SmtpFilter::incSmtpSessionsCompleted() { config_->stats_.smtp_session_completed_.inc(); }
void SmtpFilter::incActiveTransaction() { config_->stats_.smtp_transaction_active_.inc(); }
void SmtpFilter::decActiveTransaction() { config_->stats_.smtp_transaction_active_.dec(); }
void SmtpFilter::incActiveSession() { config_->stats_.smtp_session_active_.inc(); }
void SmtpFilter::decActiveSession() { config_->stats_.smtp_session_active_.dec(); }

void SmtpFilter::incSmtpSessionsTerminated() { config_->stats_.smtp_session_terminated_.inc(); }

void SmtpFilter::incSmtpConnectionEstablishmentErrors() {
  config_->stats_.smtp_connection_establishment_errors_.inc();
}

void SmtpFilter::incMailDataTransferErrors() {
  config_->stats_.smtp_mail_data_transfer_errors_.inc();
}

void SmtpFilter::incSmtpTrxnFailed() { config_->stats_.smtp_transaction_failed_.inc(); }

void SmtpFilter::incMailRcptErrors() { config_->stats_.smtp_rcpt_errors_.inc(); }
void SmtpFilter::inc4xxErrors() { config_->stats_.smtp_4xx_errors_.inc(); }
void SmtpFilter::inc5xxErrors() { config_->stats_.smtp_5xx_errors_.inc(); }

void SmtpFilter::incTlsTerminatedSessions() {
  config_->stats_.smtp_session_tls_termination_success_.inc();
}

void SmtpFilter::incTlsTerminationErrors() {
  config_->stats_.smtp_session_tls_termination_error_.inc();
}

void SmtpFilter::incSmtpAuthErrors() { config_->stats_.smtp_auth_errors_.inc(); }

void SmtpFilter::incUpstreamTlsSuccess() {
  config_->stats_.smtp_session_upstream_tls_success_.inc();
}

void SmtpFilter::incUpstreamTlsFailed() { config_->stats_.smtp_session_upstream_tls_failed_.inc(); }

void SmtpFilter::closeDownstreamConnection() {
  read_callbacks_->connection().close(Network::ConnectionCloseType::NoFlush);
  incSmtpSessionsTerminated();
}

// onData method processes payloads sent by downstream client.
Network::FilterStatus SmtpFilter::onData(Buffer::Instance& data, bool end_stream) {
  ENVOY_CONN_LOG(trace, "smtp_proxy: got {} bytes", read_callbacks_->connection(), data.length(),
                 "end_stream ", end_stream);
  read_buffer_.add(data);
  Network::FilterStatus result = doDecode(read_buffer_, false);
  if (result == Network::FilterStatus::StopIteration) {
    ASSERT(read_buffer_.length() == 0);
    data.drain(data.length());
  }
  return result;
}

// onWrite method processes payloads sent by upstream to the client.
Network::FilterStatus SmtpFilter::onWrite(Buffer::Instance& data, bool end_stream) {
  ENVOY_CONN_LOG(trace, "smtp_proxy: got {} bytes", write_callbacks_->connection(), data.length(),
                 "end_stream ", end_stream);
  write_buffer_.add(data);
  Network::FilterStatus result = doDecode(write_buffer_, true);
  if (result == Network::FilterStatus::StopIteration) {
    ASSERT(write_buffer_.length() == 0);
    data.drain(data.length());
  }
  return result;
}

Network::FilterStatus SmtpFilter::doDecode(Buffer::Instance& data, bool upstream) {

  switch (decoder_->onData(data, upstream)) {
  case SmtpUtils::Result::ReadyForNext:
    return Network::FilterStatus::Continue;
  case SmtpUtils::Result::Stopped:
    return Network::FilterStatus::StopIteration;
  default:
    break;
  }
  return Network::FilterStatus::Continue;
}

DecoderPtr SmtpFilter::createDecoder(DecoderCallbacks* callbacks, TimeSource& time_source,
                                     Random::RandomGenerator& random_generator) {
  return std::make_unique<DecoderImpl>(callbacks, time_source, random_generator);
}

} // namespace SmtpProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

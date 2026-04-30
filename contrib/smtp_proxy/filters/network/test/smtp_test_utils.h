#pragma once

#include "test/mocks/buffer/mocks.h"
#include "test/mocks/network/mocks.h"
#include "test/mocks/stream_info/mocks.h"

#include "contrib/smtp_proxy/filters/network/source/smtp_callbacks.h"
#include "contrib/smtp_proxy/filters/network/source/smtp_stats.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace SmtpProxy {

class MockDecoderCallbacks : public DecoderCallbacks {
public:
  MockDecoderCallbacks() : stats_(generateStats("test", scope_)) {
    ON_CALL(*this, getStreamInfo()).WillByDefault(testing::ReturnRef(stream_info_));
    ON_CALL(*this, sendReplyDownstream(_)).WillByDefault(testing::Return(false));
    ON_CALL(*this, connection()).WillByDefault(testing::ReturnRef(mock_connection_));
    ON_CALL(*this, getReadBuffer()).WillByDefault(testing::ReturnRef(buffer_));
    ON_CALL(*this, getStats()).WillByDefault(testing::ReturnRef(stats_));
  }

  static SmtpProxyStats generateStats(const std::string& prefix, Stats::Scope& scope) {
    return SmtpProxyStats{ALL_SMTP_PROXY_STATS(POOL_COUNTER_PREFIX(scope, prefix),
                                               POOL_GAUGE_PREFIX(scope, prefix),
                                               POOL_HISTOGRAM_PREFIX(scope, prefix))};
  }

  Stats::IsolatedStoreImpl stats_store_;
  Stats::Scope& scope_{*stats_store_.rootScope()};
  SmtpProxyStats stats_;
  NiceMock<Network::MockConnection> mock_connection_;
  NiceMock<Envoy::StreamInfo::MockStreamInfo> stream_info_;
  NiceMock<MockBuffer> buffer_;
  MOCK_METHOD(void, incSmtpTransactionRequests, (), (override));
  MOCK_METHOD(void, incSmtpTransactionsCompleted, (), (override));
  MOCK_METHOD(void, incSmtpTransactionsAborted, (), (override));
  MOCK_METHOD(void, incSmtpTrxnFailed, (), (override));
  MOCK_METHOD(void, incSmtpSessionRequests, (), (override));
  MOCK_METHOD(void, incSmtpConnectionEstablishmentErrors, (), (override));
  MOCK_METHOD(void, incSmtpSessionsCompleted, (), (override));
  MOCK_METHOD(void, incSmtpSessionsTerminated, (), (override));
  MOCK_METHOD(void, incTlsTerminatedSessions, (), (override));
  MOCK_METHOD(void, incTlsTerminationErrors, (), (override));
  MOCK_METHOD(void, incUpstreamTlsSuccess, (), (override));
  MOCK_METHOD(void, incUpstreamTlsFailed, (), (override));

  MOCK_METHOD(void, incActiveTransaction, (), (override));
  MOCK_METHOD(void, decActiveTransaction, (), (override));
  MOCK_METHOD(void, incActiveSession, (), (override));
  MOCK_METHOD(void, decActiveSession, (), (override));

  MOCK_METHOD(void, incSmtpAuthErrors, (), (override));
  MOCK_METHOD(void, incMailDataTransferErrors, (), (override));
  MOCK_METHOD(void, incMailRcptErrors, (), (override));
  MOCK_METHOD(void, inc4xxErrors, (), (override));
  MOCK_METHOD(void, inc5xxErrors, (), (override));

  MOCK_METHOD(bool, downstreamStartTls, (absl::string_view), (override));
  MOCK_METHOD(bool, sendReplyDownstream, (absl::string_view), (override));
  MOCK_METHOD(bool, sendUpstream, (Buffer::Instance&), (override));
  MOCK_METHOD(bool, upstreamTlsEnabled, (), (const, override));
  MOCK_METHOD(bool, upstreamStartTls, (), (override));
  MOCK_METHOD(bool, downstreamTlsRequired, (), (const, override));
  MOCK_METHOD(bool, upstreamTlsRequired, (), (const, override));
  MOCK_METHOD(void, closeDownstreamConnection, (), (override));
  MOCK_METHOD(bool, tracingEnabled, (), (override));
  MOCK_METHOD(bool, protocolInspectionEnabled, (), (const, override));
  MOCK_METHOD(bool, downstreamTlsEnabled, (), (const, override));
  MOCK_METHOD(MockBuffer&, getReadBuffer, (), (override));
  MOCK_METHOD(MockBuffer&, getWriteBuffer, (), (override));

  MOCK_METHOD(Network::MockConnection&, connection, (), (const));
  MOCK_METHOD(StreamInfo::MockStreamInfo&, getStreamInfo, (), (override));
  MOCK_METHOD(void, emitLogEntry, (StreamInfo::StreamInfo&), (override));
  MOCK_METHOD(SmtpProxyStats&, getStats, (), (override));
  
};


class SmtpTestUtils {
public:
  inline static const char* smtpHeloCommand = "HELO test.com\r\n";
  inline static const char* smtpEhloCommand = "EHLO test.com\r\n";
  inline static const char* smtpAuthCommand = "AUTH PLAIN AHVzZXJuYW1lAHBhc3N3b3Jk";
  inline static const char* smtpMailCommand = "MAIL FROM:<test@example.com>\r\n";
  inline static const char* smtpRcptCommand = "RCPT TO:<test@example.com>\r\n";
  inline static const char* smtpBdatCommand = "BDAT 1024\r\n";
  inline static const char* smtpBdatLastCommand = "BDAT 2048 LAST\r\n";
};

} // namespace SmtpProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

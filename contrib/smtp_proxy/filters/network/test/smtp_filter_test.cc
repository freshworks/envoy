#include <vector>

#include "source/common/buffer/buffer_impl.h"

#include "test/mocks/common.h"
#include "test/mocks/network/mocks.h"

#include "contrib/smtp_proxy/filters/network/source/smtp_filter.h"
#include "contrib/smtp_proxy/filters/network/source/smtp_utils.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using testing::NiceMock;

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace SmtpProxy {

// Enum for SSL configuration types
enum class SslConfig {
  DISABLE,
  ENABLE,
  REQUIRE
};

class SmtpFilterTest : public testing::Test {
public:
  // Constructor to set up SSL configuration
  SmtpFilterTest(SslConfig downstream_ssl = SslConfig::DISABLE, 
                 SslConfig upstream_ssl = SslConfig::DISABLE,
                 bool protocol_inspection = false)
      : downstream_ssl_(downstream_ssl), upstream_ssl_(upstream_ssl), 
        protocol_inspection_(protocol_inspection) {}

  void SetUp() override {
    // Convert enum to protobuf enum
    auto downstream_ssl_enum = convertSslConfig(downstream_ssl_);
    auto upstream_ssl_enum = convertSslConfig(upstream_ssl_);
    
    SmtpFilterConfig::SmtpFilterConfigOptions config_options{
        stat_prefix_, tracing_, downstream_ssl_enum, upstream_ssl_enum,
        protocol_inspection_, access_logs_};
    
    config_ = std::make_shared<SmtpFilterConfig>(config_options, scope_);
    filter_ = std::make_unique<SmtpFilter>(config_, time_source_, random_);
    filter_->initializeReadFilterCallbacks(filter_callbacks_);
  }

  // Helper method to convert SslConfig enum to protobuf enum
  envoy::extensions::filters::network::smtp_proxy::v3alpha::SmtpProxy::SSLMode 
  convertSslConfig(SslConfig config) {
    switch (config) {
      case SslConfig::DISABLE:
        return envoy::extensions::filters::network::smtp_proxy::v3alpha::SmtpProxy::DISABLE;
      case SslConfig::ENABLE:
        return envoy::extensions::filters::network::smtp_proxy::v3alpha::SmtpProxy::ENABLE;
      case SslConfig::REQUIRE:
        return envoy::extensions::filters::network::smtp_proxy::v3alpha::SmtpProxy::REQUIRE;
      default:
        return envoy::extensions::filters::network::smtp_proxy::v3alpha::SmtpProxy::DISABLE;
    }
  }

  // Helper methods to check SSL configuration
  bool isDownstreamSslEnabled() const {
    return downstream_ssl_ == SslConfig::ENABLE || downstream_ssl_ == SslConfig::REQUIRE;
  }

  bool isDownstreamSslRequired() const {
    return downstream_ssl_ == SslConfig::REQUIRE;
  }

  bool isUpstreamSslEnabled() const {
    return upstream_ssl_ == SslConfig::ENABLE || upstream_ssl_ == SslConfig::REQUIRE;
  }

  bool isUpstreamSslRequired() const {
    return upstream_ssl_ == SslConfig::REQUIRE;
  }

  bool isProtocolInspectionEnabled() const {
    return protocol_inspection_;
  }

  // // Helper method to create a test with specific SSL configuration
  // static std::unique_ptr<SmtpFilterTest> createTest(SslConfig downstream_ssl = SslConfig::DISABLE,
  //                                                  SslConfig upstream_ssl = SslConfig::DISABLE,
  //                                                  bool protocol_inspection = false) {
  //   auto test = std::make_unique<SmtpFilterTest>(downstream_ssl, upstream_ssl, protocol_inspection);
  //   test->SetUp();
  //   return test;
  // }

protected:
  SslConfig downstream_ssl_;
  SslConfig upstream_ssl_;
  bool protocol_inspection_;
  
  SmtpFilterConfigSharedPtr config_;
  std::string stat_prefix_{"test."};
  bool tracing_{false};
  const std::vector<AccessLog::InstanceSharedPtr> access_logs_;

  Stats::IsolatedStoreImpl store_;
  Stats::Scope& scope_{*store_.rootScope()};
  NiceMock<Network::MockReadFilterCallbacks> filter_callbacks_;
  NiceMock<Network::MockConnection> connection_;
  Buffer::OwnedImpl data_;
  NiceMock<MockTimeSystem> time_source_;
  NiceMock<Random::MockRandomGenerator> random_;
  std::unique_ptr<SmtpFilter> filter_;
};

// Test classes for different SSL configurations
class SmtpFilterTestPlainText : public SmtpFilterTest {
public:
  SmtpFilterTestPlainText() : SmtpFilterTest(SslConfig::DISABLE, SslConfig::DISABLE) {}
};

class SmtpFilterTestDownstreamSsl : public SmtpFilterTest {
public:
  SmtpFilterTestDownstreamSsl() : SmtpFilterTest(SslConfig::ENABLE, SslConfig::DISABLE) {}
};

class SmtpFilterTestUpstreamSsl : public SmtpFilterTest {
public:
  SmtpFilterTestUpstreamSsl() : SmtpFilterTest(SslConfig::DISABLE, SslConfig::ENABLE) {}
};

class SmtpFilterTestBothSsl : public SmtpFilterTest {
public:
  SmtpFilterTestBothSsl() : SmtpFilterTest(SslConfig::ENABLE, SslConfig::ENABLE) {}
};

class SmtpFilterTestRequiredSsl : public SmtpFilterTest {
public:
  SmtpFilterTestRequiredSsl() : SmtpFilterTest(SslConfig::REQUIRE, SslConfig::REQUIRE) {}
};

// Test New Session counter increment
TEST_F(SmtpFilterTest, NewSessionStatsTest) {
  EXPECT_EQ(Envoy::Network::FilterStatus::Continue, filter_->onNewConnection());
  EXPECT_EQ(filter_->getSession()->getState(), SmtpSession::State::ConnectionRequest);
  EXPECT_EQ(1, config_->stats().session_requests_.value());
}

// Test with different SSL configurations
TEST_F(SmtpFilterTestPlainText, PlainTextSessionTest) {
  EXPECT_FALSE(isDownstreamSslEnabled());
  EXPECT_FALSE(isUpstreamSslEnabled());
  EXPECT_EQ(Envoy::Network::FilterStatus::Continue, filter_->onNewConnection());
  EXPECT_EQ(filter_->getSession()->getState(), SmtpSession::State::ConnectionRequest);
}

TEST_F(SmtpFilterTestDownstreamSsl, DownstreamSslSessionTest) {
  EXPECT_TRUE(isDownstreamSslEnabled());
  EXPECT_FALSE(isUpstreamSslEnabled());
  EXPECT_EQ(Envoy::Network::FilterStatus::Continue, filter_->onNewConnection());
  EXPECT_EQ(filter_->getSession()->getState(), SmtpSession::State::ConnectionRequest);
}

TEST_F(SmtpFilterTestUpstreamSsl, UpstreamSslSessionTest) {
  EXPECT_FALSE(isDownstreamSslEnabled());
  EXPECT_TRUE(isUpstreamSslEnabled());
  EXPECT_EQ(Envoy::Network::FilterStatus::Continue, filter_->onNewConnection());
  EXPECT_EQ(filter_->getSession()->getState(), SmtpSession::State::ConnectionRequest);
}

TEST_F(SmtpFilterTestBothSsl, BothSslSessionTest) {
  EXPECT_TRUE(isDownstreamSslEnabled());
  EXPECT_TRUE(isUpstreamSslEnabled());
  EXPECT_EQ(Envoy::Network::FilterStatus::Continue, filter_->onNewConnection());
  EXPECT_EQ(filter_->getSession()->getState(), SmtpSession::State::ConnectionRequest);
}

TEST_F(SmtpFilterTestRequiredSsl, RequiredSslSessionTest) {
  EXPECT_TRUE(isDownstreamSslRequired());
  EXPECT_TRUE(isUpstreamSslRequired());
  EXPECT_EQ(Envoy::Network::FilterStatus::Continue, filter_->onNewConnection());
  EXPECT_EQ(filter_->getSession()->getState(), SmtpSession::State::ConnectionRequest);
}

TEST_F(SmtpFilterTestDownstreamSsl, TestDownstreamStarttls) {

  EXPECT_FALSE(isUpstreamSslEnabled());
  ASSERT_FALSE(filter_->upstreamTlsEnabled());
  EXPECT_EQ(Envoy::Network::FilterStatus::Continue, filter_->onNewConnection());
  // Upstream TLS is disabled, testing only downstream starttls handling
  // filter_->getConfig()->upstream_tls_ =
  //     envoy::extensions::filters::network::smtp_proxy::v3alpha::SmtpProxy::DISABLE;
  filter_->getSession()->setState(SmtpSession::State::ConnectionSuccess);

  data_.add("EHLO localhost\r\n");
  // std::cout << data_.toString() << std::endl;
  ASSERT_THAT(Network::FilterStatus::Continue, filter_->onData(data_, false));
  EXPECT_EQ(SmtpSession::State::SessionInitRequest, filter_->getSession()->getState());

  data_.drain(data_.length());

  data_.add("250-Hello localhost\r\n250-PIPELINING\r\n250-8BITMIME\r\n250-STARTTLS\r\n");
  ASSERT_THAT(Network::FilterStatus::Continue, filter_->onWrite(data_, false));

  filter_->getSession()->setState(SmtpSession::State::SessionInProgress);
  data_.drain(data_.length());
  data_.add("STARTTLS\r\n");

  // Downstream TLS termination successful after STARTTLS

  EXPECT_CALL(filter_callbacks_, connection()).WillRepeatedly(ReturnRef(connection_));
  Network::Connection::BytesSentCb cb;
  EXPECT_CALL(connection_, addBytesSentCallback(_)).WillOnce(testing::SaveArg<0>(&cb));
  Buffer::OwnedImpl buf;
  EXPECT_CALL(connection_, write(_, false)).WillOnce(testing::SaveArg<0>(&buf));

  std::string resp = "220 2.0.0 Ready to start TLS\r\n";
  ASSERT_THAT(Network::FilterStatus::StopIteration, filter_->onData(data_, false));
  ASSERT_STREQ(resp.c_str(), buf.toString().c_str());

  // Now indicate through the callback that 220 response has been sent.
  // Filter should call startSecureTransport and should not close the connection.
  EXPECT_CALL(connection_, startSecureTransport()).WillOnce(testing::Return(true));
  EXPECT_CALL(connection_, close(_)).Times(0);
  cb(buf.length());

  EXPECT_EQ(SmtpSession::State::SessionInProgress, filter_->getSession()->getState());
  EXPECT_EQ(config_->stats().downstream_tls_termination_success_.value(), 1);

  // Send starttls command again, receive 503 out of order command response from filter.
  buf.drain(buf.length());
  data_.add("STARTTLS\r\n");

  EXPECT_CALL(connection_, addBytesSentCallback(_)).WillOnce(testing::SaveArg<0>(&cb));
  EXPECT_CALL(connection_, write(_, false)).WillOnce(testing::SaveArg<0>(&buf));
  resp = "502 5.5.1 Already running in TLS\r\n";
  ASSERT_THAT(Network::FilterStatus::StopIteration, filter_->onData(data_, false));
  ASSERT_STREQ(resp.c_str(), buf.toString().c_str());
}

/*
TEST_F(SmtpFilterTest, TestSendReplyDownstream) {
  // initialize();

  EXPECT_CALL(filter_callbacks_, connection()).WillRepeatedly(ReturnRef(connection_));
  Network::Connection::BytesSentCb cb;
  EXPECT_CALL(connection_, addBytesSentCallback(_)).WillOnce(testing::SaveArg<0>(&cb));
  Buffer::OwnedImpl buf;
  EXPECT_CALL(connection_, write(_, false)).WillOnce(testing::SaveArg<0>(&buf));

  ASSERT_THAT(false, filter_->sendReplyDownstream(SmtpUtils::mailboxUnavailableResponse));

  ASSERT_STREQ(SmtpUtils::mailboxUnavailableResponse, buf.toString().c_str());

  filter_callbacks_.connection().close(Network::ConnectionCloseType::NoFlush);

  ASSERT_THAT(true, filter_->sendReplyDownstream(SmtpUtils::mailboxUnavailableResponse));
}

TEST_F(SmtpFilterTest, TestUpstreamStartTls) {
  EXPECT_EQ(Envoy::Network::FilterStatus::Continue, filter_->onNewConnection());
  // Upstream TLS is disabled, testing only downstream starttls handling
  filter_->getConfig()->upstream_tls_ =
      envoy::extensions::filters::network::smtp_proxy::v3alpha::SmtpProxy::REQUIRE;
  ASSERT_TRUE(filter_->upstreamTlsEnabled());
  filter_->getSession()->setState(SmtpSession::State::ConnectionSuccess);

  data_.add("EHLO localhost\r\n");
  ASSERT_THAT(Network::FilterStatus::Continue, filter_->onData(data_, false));
  EXPECT_EQ(SmtpSession::State::SessionInitRequest, filter_->getSession()->getState());

  data_.drain(data_.length());

  data_.add("250-Hello localhost\r\n250-PIPELINING\r\n250-8BITMIME\r\n250-STARTTLS\r\n");
  ASSERT_THAT(Network::FilterStatus::Continue, filter_->onWrite(data_, false));
  EXPECT_EQ(SmtpSession::State::SessionInProgress, filter_->getSession()->getState());
  data_.drain(data_.length());
  data_.add("STARTTLS\r\n");

  // Upstream TLS termination successful after STARTTLS

  ASSERT_THAT(Network::FilterStatus::Continue, filter_->onData(data_, false));
  ASSERT_EQ(SmtpSession::State::UpstreamTlsNegotiation, filter_->getSession()->getState());

  data_.drain(data_.length());
  data_.add("220 Ready to start TLS\r\n");

  EXPECT_CALL(filter_callbacks_, startUpstreamSecureTransport()).WillOnce(testing::Return(true));

  ASSERT_THAT(Network::FilterStatus::StopIteration, filter_->onWrite(data_, false));

  EXPECT_CALL(connection_, close(_)).Times(0);
  EXPECT_EQ(config_->stats().upstream_tls_success_.value(), 1);

  filter_->getSession()->setSessionEncrypted(false);
  filter_->getSession()->setState(SmtpSession::State::UpstreamTlsNegotiation);

  data_.drain(data_.length());
  data_.add("220 Ready to start TLS\r\n");

  EXPECT_CALL(filter_callbacks_, startUpstreamSecureTransport()).WillOnce(testing::Return(false));

  ASSERT_THAT(Network::FilterStatus::StopIteration, filter_->onWrite(data_, false));
  ASSERT_EQ(SmtpSession::State::SessionTerminated, filter_->getSession()->getState());
  EXPECT_EQ(config_->stats().upstream_tls_error_.value(), 1);
}
*/

} // namespace SmtpProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

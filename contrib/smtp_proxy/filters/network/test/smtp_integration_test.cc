#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "contrib/smtp_proxy/filters/network/source/smtp_decoder.h"
#include "contrib/smtp_proxy/filters/network/source/smtp_utils.h"

#include "test/test_common/network_utility.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace SmtpProxy {


class SmtpBaseIntegrationTest : public testing::TestWithParam<Network::Address::IpVersion>,
                                    public BaseIntegrationTest {
public:
  // Tuple to store upstream and downstream startTLS configuration.
  // The first string contains string to enable/disable SSL.
  // The second string contains transport socket configuration.
  using SSLConfig = std::tuple<const absl::string_view, const absl::string_view>;

  std::string smtpConfig(SSLConfig downstream_ssl_config, SSLConfig upstream_ssl_config,
                             std::string additional_filters) {
    std::string main_config = fmt::format(
        TestEnvironment::readFileToStringForTest(TestEnvironment::runfilesPath(
            "contrib/smtp_proxy/filters/network/test/smtp_test_config.yaml-template")),
        Platform::null_device_path, Network::Test::getLoopbackAddressString(GetParam()),
        Network::Test::getLoopbackAddressString(GetParam()),
        std::get<1>(upstream_ssl_config), // upstream SSL transport socket
        Network::Test::getAnyAddressString(GetParam()),
        std::get<0>(downstream_ssl_config),  // downstream SSL termination
        std::get<0>(upstream_ssl_config),    // upstream_SSL option
        additional_filters,                  // additional filters to insert after smtp
        std::get<1>(downstream_ssl_config)); // downstream SSL transport socket

    return main_config;
  }

  SmtpBaseIntegrationTest(SSLConfig downstream_ssl_config, SSLConfig upstream_ssl_config,
                              std::string additional_filters = "")
      : BaseIntegrationTest(GetParam(), smtpConfig(downstream_ssl_config, upstream_ssl_config,
                                                       additional_filters)) {
    skip_tag_extraction_rule_check_ = true;
  };

  void SetUp() override { BaseIntegrationTest::initialize(); }

  static constexpr absl::string_view empty_config_string_{""};
  static constexpr SSLConfig NoUpstreamSSL{empty_config_string_, empty_config_string_};
  static constexpr SSLConfig NoDownstreamSSL{empty_config_string_, empty_config_string_};
  FakeRawConnectionPtr fake_upstream_connection_;
};

// Base class for tests with `terminate_ssl` disabled and without
// `starttls` transport socket.
class BasicSmtpIntegrationTest : public SmtpBaseIntegrationTest {
public:
  BasicSmtpIntegrationTest() : SmtpBaseIntegrationTest(NoDownstreamSSL, NoUpstreamSSL) {}
};


INSTANTIATE_TEST_SUITE_P(IpVersions, BasicSmtpIntegrationTest,
                         testing::ValuesIn(TestEnvironment::getIpVersionsForTest()));
// Test that the filter is properly chained and reacts to successful login
// message.
TEST_P(BasicSmtpIntegrationTest, Login) {
  std::string str;
  std::string recv;

  IntegrationTcpClientPtr tcp_client = makeTcpConnection(lookupPort("listener_0"));
  FakeRawConnectionPtr fake_upstream_connection;
  ASSERT_TRUE(fake_upstreams_[0]->waitForRawConnection(fake_upstream_connection));

  // Send the startup message.
  Buffer::OwnedImpl data;
  std::string rcvd;
  char buf[32];

  memset(buf, 0, sizeof(buf));
  // Add length.
  data.writeBEInt<uint32_t>(12);
  // Add 8 bytes of some data.
  data.add(buf, 8);
  ASSERT_TRUE(tcp_client->write(data.toString()));
  ASSERT_TRUE(fake_upstream_connection->waitForData(data.toString().length(), &rcvd));
  data.drain(data.length());

  // TCP session is up. Just send the AuthenticationOK downstream.
  data.add("R");
  // Add length.
  data.writeBEInt<uint32_t>(8);
  uint32_t code = 0;
  data.add(&code, sizeof(code));

  rcvd.clear();
  ASSERT_TRUE(fake_upstream_connection->write(data.toString()));
  rcvd.append(data.toString());
  tcp_client->waitForData(rcvd, true);

  tcp_client->close();
  ASSERT_TRUE(fake_upstream_connection->waitForDisconnect());

  // Make sure that the successful login bumped up the number of sessions.
  test_server_->waitForCounterEq("smtp.smtp_stats.sessions", 1);
}



} // namespace SmtpProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
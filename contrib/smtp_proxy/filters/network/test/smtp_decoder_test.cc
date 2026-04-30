#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "test/mocks/common.h"
#include "test/mocks/network/mocks.h"

#include "contrib/smtp_proxy/filters/network/source/smtp_decoder_impl.h"
#include "contrib/smtp_proxy/filters/network/source/smtp_utils.h"

using testing::_;
using testing::Eq;
using testing::Invoke;
using testing::NiceMock;
using testing::Ref;
using testing::Return;
using testing::ReturnRef;

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace SmtpProxy {

class DecoderImplTest : public ::testing::Test {
public:
  void SetUp() override {
    decoder_ = std::make_unique<DecoderImpl>();
  }

protected:
  Buffer::OwnedImpl data_;
  std::unique_ptr<DecoderImpl> decoder_;
};

// Tests for isValidSmtpLine function
TEST_F(DecoderImplTest, TestIsValidSmtpLine) {
  std::string output;
  
  // Valid complete line
  data_.add("EHLO localhost\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, 
            decoder_->isValidSmtpLine(data_, SmtpUtils::maxCommandLen, output));
  EXPECT_EQ("EHLO localhost\r\n", output);
  data_.drain(data_.length());
  output.clear();
  
  // Valid line with max length
  std::string longCommand = std::string(SmtpUtils::maxCommandLen - 2, 'A') + "\r\n";
  data_.add(longCommand);
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, 
            decoder_->isValidSmtpLine(data_, SmtpUtils::maxCommandLen, output));
  EXPECT_EQ(longCommand, output);
  data_.drain(data_.length());
  output.clear();
  
  // Need more data
  data_.add("EHLO localhost"); // Missing CRLF
  EXPECT_EQ(SmtpUtils::Result::NeedMoreData, 
            decoder_->isValidSmtpLine(data_, SmtpUtils::maxCommandLen, output));
  EXPECT_TRUE(output.empty());
  data_.drain(data_.length());
  output.clear();
  
  // Protocol error - exceeds max length
  std::string tooLongCommand = std::string(SmtpUtils::maxCommandLen + 1, 'A');
  data_.add(tooLongCommand);
  EXPECT_EQ(SmtpUtils::Result::ProtocolError, 
            decoder_->isValidSmtpLine(data_, SmtpUtils::maxCommandLen, output));
  data_.drain(data_.length());
  output.clear();
  
  // Protocol error - empty line
  data_.add("\r\n"); // Empty line
  EXPECT_EQ(SmtpUtils::Result::ProtocolError, 
            decoder_->isValidSmtpLine(data_, SmtpUtils::maxCommandLen, output));
  data_.drain(data_.length());
  output.clear();
  
  // Protocol error - only CRLF
  data_.add("\r\n\r\n"); // Multiple CRLF without content
  EXPECT_EQ(SmtpUtils::Result::ProtocolError, 
            decoder_->isValidSmtpLine(data_, SmtpUtils::maxCommandLen, output));
  data_.drain(data_.length());
  output.clear();
}

// Tests for parseCommand function
TEST_F(DecoderImplTest, TestParseCommand) {
  Decoder::Command command;
  
  // Valid command with args
  data_.add("EHLO localhost\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("EHLO", command.verb);
  EXPECT_EQ("localhost", command.args);
  EXPECT_EQ(16, command.len);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // Valid command without args
  data_.add("QUIT\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("QUIT", command.verb);
  EXPECT_EQ("", command.args);
  EXPECT_EQ(6, command.len);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // Valid command with multiple spaces
  data_.add("MAIL FROM:<test@example.com>\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("MAIL", command.verb);
  EXPECT_EQ("FROM:<test@example.com>", command.args);
  EXPECT_EQ(30, command.len);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // Invalid command with leading spaces
  data_.add("  EHLO localhost\r\n");
  EXPECT_EQ(SmtpUtils::Result::ProtocolError, decoder_->parseCommand(data_, command));
  EXPECT_EQ("", command.verb);
  EXPECT_EQ("", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // Valid command with trailing spaces
  data_.add("EHLO localhost  \r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("EHLO", command.verb);
  EXPECT_EQ("localhost  ", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // Need more data
  data_.add("EHLO localhost"); // Missing CRLF
  EXPECT_EQ(SmtpUtils::Result::NeedMoreData, decoder_->parseCommand(data_, command));
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // Protocol error - exceeds max length
  std::string tooLongCommand = std::string(SmtpUtils::maxCommandLen + 1, 'A');
  data_.add(tooLongCommand);
  EXPECT_EQ(SmtpUtils::Result::ProtocolError, decoder_->parseCommand(data_, command));
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // Protocol error - empty line
  data_.add("\r\n");
  EXPECT_EQ(SmtpUtils::Result::ProtocolError, decoder_->parseCommand(data_, command));
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // Protocol error - only CRLF
  data_.add("\r\n\r\n");
  EXPECT_EQ(SmtpUtils::Result::ProtocolError, decoder_->parseCommand(data_, command));
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // Protocol error - data after CRLF
  data_.add("EHLO localhost\r\nEXTRA DATA");
  EXPECT_EQ(SmtpUtils::Result::ProtocolError, decoder_->parseCommand(data_, command));
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // Edge case - max length command
  std::string maxLengthCommand = std::string(SmtpUtils::maxCommandLen - 2, 'A') + "\r\n";
  data_.add(maxLengthCommand);
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ(std::string(SmtpUtils::maxCommandLen - 2, 'A'), command.verb);
  EXPECT_EQ("", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // Stress test - very long command
  std::string veryLongCommand = std::string(10000, 'A') + "\r\n";
  data_.add(veryLongCommand);
  EXPECT_EQ(SmtpUtils::Result::ProtocolError, decoder_->parseCommand(data_, command));
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // HELO command
  data_.add("HELO example.com\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("HELO", command.verb);
  EXPECT_EQ("example.com", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // EHLO command
  data_.add("EHLO example.com\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("EHLO", command.verb);
  EXPECT_EQ("example.com", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // MAIL FROM command
  data_.add("MAIL FROM:<sender@example.com>\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("MAIL", command.verb);
  EXPECT_EQ("FROM:<sender@example.com>", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // RCPT TO command
  data_.add("RCPT TO:<recipient@example.com>\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("RCPT", command.verb);
  EXPECT_EQ("TO:<recipient@example.com>", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // DATA command
  data_.add("DATA\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("DATA", command.verb);
  EXPECT_EQ("", command.args);
  data_.drain(data_.length());
  command = Decoder::Command();
  
  // QUIT command
  data_.add("QUIT\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("QUIT", command.verb);
  EXPECT_EQ("", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // RSET command
  data_.add("RSET\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("RSET", command.verb);
  EXPECT_EQ("", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // AUTH command
  data_.add("AUTH PLAIN dGVzdAB0ZXN0\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("AUTH", command.verb);
  EXPECT_EQ("PLAIN dGVzdAB0ZXN0", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // STARTTLS command
  data_.add("STARTTLS\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("STARTTLS", command.verb);
  EXPECT_EQ("", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // BDAT command
  data_.add("BDAT 1024\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("BDAT", command.verb);
  EXPECT_EQ("1024", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // BDAT command with LAST
  data_.add("BDAT 2048 LAST\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("BDAT", command.verb);
  EXPECT_EQ("2048 LAST", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // Valid BDAT command with chunk size only
  data_.add("BDAT 1024\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("BDAT", command.verb);
  EXPECT_EQ("1024", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // Valid BDAT command with chunk size and LAST
  data_.add("BDAT 2048 LAST\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("BDAT", command.verb);
  EXPECT_EQ("2048 LAST", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // Valid BDAT command with large chunk size
  data_.add("BDAT 1048576\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("BDAT", command.verb);
  EXPECT_EQ("1048576", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // Valid BDAT command with chunk size 1
  data_.add("BDAT 1\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("BDAT", command.verb);
  EXPECT_EQ("1", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // BDAT without arguments
  data_.add("BDAT\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("BDAT", command.verb);
  EXPECT_EQ("", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // BDAT with invalid chunk size (non-numeric)
  data_.add("BDAT abc\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("BDAT", command.verb);
  EXPECT_EQ("abc", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // BDAT with negative chunk size
  data_.add("BDAT -1024\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("BDAT", command.verb);
  EXPECT_EQ("-1024", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // BDAT with zero chunk size
  data_.add("BDAT 0\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("BDAT", command.verb);
  EXPECT_EQ("0", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
  
  // BDAT with extra arguments
  data_.add("BDAT 1024 LAST EXTRA\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseCommand(data_, command));
  EXPECT_EQ("BDAT", command.verb);
  EXPECT_EQ("1024 LAST EXTRA", command.args);
  data_.drain(data_.length());
  command = Decoder::Command(); 
}

// Tests for parseResponse function
TEST_F(DecoderImplTest, TestParseResponse) {
  Decoder::Response response;
  
  // Single line response
  data_.add("220 Hi! This is upstream.com mail server\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseResponse(data_, response));
  EXPECT_EQ(220, response.resp_code);
  EXPECT_EQ("Hi! This is upstream.com mail server", response.msg);
  EXPECT_EQ(42, response.len);
  data_.drain(data_.length());
  
  // Multi-line response
  data_.add("250-EHLO localhost\r\n250-AUTH PLAIN\r\n250 STARTTLS\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseResponse(data_, response));
  EXPECT_EQ(250, response.resp_code);
  EXPECT_EQ("EHLO localhost\r\nAUTH PLAIN\r\nSTARTTLS", response.msg);
  data_.drain(data_.length());
  
  // Multi-line response with spaces
  data_.add("250-First line\r\n250-Second line\r\n250 Final line\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseResponse(data_, response));
  EXPECT_EQ(250, response.resp_code);
  EXPECT_EQ("First line\r\nSecond line\r\nFinal line", response.msg);
  data_.drain(data_.length());
  
  // Response with dash separator
  data_.add("250-This is a continuation line\r\n250 End of response\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseResponse(data_, response));
  EXPECT_EQ(250, response.resp_code);
  EXPECT_EQ("This is a continuation line\r\nEnd of response", response.msg);
  data_.drain(data_.length());
  
  // Response with space separator
  data_.add("250 This is a single line response\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseResponse(data_, response));
  EXPECT_EQ(250, response.resp_code);
  EXPECT_EQ("This is a single line response", response.msg);
  data_.drain(data_.length());
  
  // Need more data
  data_.add("220 Hi! This is upstream.com mail server"); // Missing CRLF
  EXPECT_EQ(SmtpUtils::Result::NeedMoreData, decoder_->parseResponse(data_, response));
  data_.drain(data_.length());
  
  // Protocol error - too short
  data_.add("22\r\n"); // Less than 3 characters
  EXPECT_EQ(SmtpUtils::Result::ProtocolError, decoder_->parseResponse(data_, response));
  data_.drain(data_.length());
  
  // Protocol error - invalid response code
  data_.add("ABC Invalid response code\r\n");
  EXPECT_EQ(SmtpUtils::Result::ProtocolError, decoder_->parseResponse(data_, response));
  data_.drain(data_.length());
  
  // Protocol error - invalid separator
  data_.add("250*Invalid separator\r\n");
  EXPECT_EQ(SmtpUtils::Result::ProtocolError, decoder_->parseResponse(data_, response));
  data_.drain(data_.length());
  
  // Protocol error - mismatched response codes
  data_.add("250-First line\r\n251-Second line\r\n250 Final line\r\n");
  EXPECT_EQ(SmtpUtils::Result::ProtocolError, decoder_->parseResponse(data_, response));
  data_.drain(data_.length());
  
  // Protocol error - empty line
  data_.add("\r\n");
  EXPECT_EQ(SmtpUtils::Result::ProtocolError, decoder_->parseResponse(data_, response));
  data_.drain(data_.length());
  
  // Protocol error - only CRLF
  data_.add("\r\n\r\n");
  EXPECT_EQ(SmtpUtils::Result::ProtocolError, decoder_->parseResponse(data_, response));
  data_.drain(data_.length());
  
  // Protocol error - exceeds max length
  std::string tooLongResponse = std::string(SmtpUtils::maxResponseLen + 1, 'A');
  data_.add(tooLongResponse);
  EXPECT_EQ(SmtpUtils::Result::ProtocolError, decoder_->parseResponse(data_, response));
  data_.drain(data_.length());
  
  // Edge case - max length response
  std::string maxLengthResponse = "250 " + std::string(SmtpUtils::maxResponseLen - 6, 'A') + "\r\n";
  data_.add(maxLengthResponse);
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseResponse(data_, response));
  EXPECT_EQ(250, response.resp_code);
  EXPECT_EQ(std::string(SmtpUtils::maxResponseLen - 6, 'A'), response.msg);
  data_.drain(data_.length());
  
  // Stress test - very long response
  std::string veryLongResponse = "250 " + std::string(10000, 'A') + "\r\n";
  data_.add(veryLongResponse);
  EXPECT_EQ(SmtpUtils::Result::ProtocolError, decoder_->parseResponse(data_, response));
  data_.drain(data_.length());
  
  // 220 - Service ready
  data_.add("220 smtp.example.com ESMTP Postfix\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseResponse(data_, response));
  EXPECT_EQ(220, response.resp_code);
  EXPECT_EQ("smtp.example.com ESMTP Postfix", response.msg);
  data_.drain(data_.length());
  
  // 250 - Requested mail action okay
  data_.add("250 Ok\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseResponse(data_, response));
  EXPECT_EQ(250, response.resp_code);
  EXPECT_EQ("Ok", response.msg);
  data_.drain(data_.length());
  
  // 334 - Server challenge
  data_.add("334 VXNlcm5hbWU6\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseResponse(data_, response));
  EXPECT_EQ(334, response.resp_code);
  EXPECT_EQ("VXNlcm5hbWU6", response.msg);
  data_.drain(data_.length());
  
  // 354 - Start mail input
  data_.add("354 End data with <CR><LF>.<CR><LF>\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseResponse(data_, response));
  EXPECT_EQ(354, response.resp_code);
  EXPECT_EQ("End data with <CR><LF>.<CR><LF>", response.msg);
  data_.drain(data_.length());
  
  // 421 - Service shutting down
  data_.add("421 smtp.example.com Service closing transmission channel\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseResponse(data_, response));
  EXPECT_EQ(421, response.resp_code);
  EXPECT_EQ("smtp.example.com Service closing transmission channel", response.msg);
  data_.drain(data_.length());
  
  // 500 - Syntax error
  data_.add("500 Syntax error, command unrecognized\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseResponse(data_, response));
  EXPECT_EQ(500, response.resp_code);
  EXPECT_EQ("Syntax error, command unrecognized", response.msg);
  data_.drain(data_.length());
  
  // 501 - Syntax error in parameters
  data_.add("501 Syntax error in parameters or arguments\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseResponse(data_, response));
  EXPECT_EQ(501, response.resp_code);
  EXPECT_EQ("Syntax error in parameters or arguments", response.msg);
  data_.drain(data_.length());
  
  // 502 - Command not implemented
  data_.add("502 Command not implemented\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseResponse(data_, response));
  EXPECT_EQ(502, response.resp_code);
  EXPECT_EQ("Command not implemented", response.msg);
  data_.drain(data_.length());
  
  // 503 - Bad sequence of commands
  data_.add("503 Bad sequence of commands\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseResponse(data_, response));
  EXPECT_EQ(503, response.resp_code);
  EXPECT_EQ("Bad sequence of commands", response.msg);
  data_.drain(data_.length());
  
  // 504 - Command parameter not implemented
  data_.add("504 Command parameter not implemented\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseResponse(data_, response));
  EXPECT_EQ(504, response.resp_code);
  EXPECT_EQ("Command parameter not implemented", response.msg);
  data_.drain(data_.length());
  
  // 535 - Authentication failed
  data_.add("535 Authentication failed\r\n");
  EXPECT_EQ(SmtpUtils::Result::ReadyForNext, decoder_->parseResponse(data_, response));
  EXPECT_EQ(535, response.resp_code);
  EXPECT_EQ("Authentication failed", response.msg);
  data_.drain(data_.length());
}


} // namespace SmtpProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

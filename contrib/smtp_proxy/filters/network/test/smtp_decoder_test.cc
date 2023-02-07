#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "contrib/smtp_proxy/filters/network/source/smtp_decoder.h"
#include "contrib/smtp_proxy/filters/network/source/smtp_utils.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace SmtpProxy {

class MockDecoderCallbacks : public DecoderCallbacks {
public:
    MOCK_METHOD(void, incSmtpTransactions, (), (override));
    MOCK_METHOD(void, incSmtpTransactionsAborted, (), (override));
    MOCK_METHOD(void, incSmtpSessionRequests, (), (override));
    MOCK_METHOD(void, incSmtpConnectionEstablishmentErrors, (), (override));
    MOCK_METHOD(void, incSmtpSessionsCompleted, (), (override));
    MOCK_METHOD(void, incSmtpSessionsTerminated, (), (override));
    MOCK_METHOD(void, incTlsTerminatedSessions, (), (override));
    MOCK_METHOD(void, incTlsTerminationErrors, (), (override));
    MOCK_METHOD(void, incUpstreamTlsSuccess, (), (override));
    MOCK_METHOD(void, incUpstreamTlsFailed, (), (override));

    MOCK_METHOD(void, incSmtpAuthErrors, (), (override));
    MOCK_METHOD(void, incMailDataTransferErrors, (), (override));
    MOCK_METHOD(void, incMailRcptErrors, (), (override));
    
    MOCK_METHOD(bool, downstreamStartTls, (absl::string_view), (override));
    MOCK_METHOD(bool, sendReplyDownstream, (absl::string_view), (override));
    MOCK_METHOD(bool, upstreamTlsRequired, (), (const));
    MOCK_METHOD(bool, upstreamStartTls, (), (override));
    MOCK_METHOD(void, closeDownstreamConnection, (), (override));
};

class DecoderImplTest : public ::testing::Test {
public:
    void SetUp() override {
        data_ = std::make_unique<Buffer::OwnedImpl>();
        // session_ = std::make_unique<SmtpSession>();
        decoder_impl_ = std::make_unique<DecoderImpl>(&callbacks_);
    }

protected:
    std::unique_ptr<Buffer::OwnedImpl> data_;
    ::testing::NiceMock<MockDecoderCallbacks> callbacks_;
    // std::unique_ptr<SmtpSession> session_;
    std::unique_ptr<DecoderImpl> decoder_impl_;
};

TEST_F(DecoderImplTest, MessageSizeInsufficient) {
    // Test case for insufficient message size
    data_->add("HELL", 4);
    // session_->setState(SmtpSession::State::CONNECTION_SUCCESS);
    EXPECT_EQ(Decoder::Result::ReadyForNext, decoder_impl_->parseCommand(*data_));
}

TEST_F(DecoderImplTest, EhloHeloCommandsTest) {
    // Test case for EHLO/HELO command
    data_->add("EHLO test.com\r\n", 14);
    decoder_impl_->getSession().setState(SmtpSession::State::CONNECTION_SUCCESS);
    EXPECT_EQ(Decoder::Result::ReadyForNext, decoder_impl_->parseCommand(*data_));
    EXPECT_EQ(SmtpSession::State::SESSION_INIT_REQUEST, decoder_impl_->getSession().getState());

    data_->drain(14);
    data_->add("HELO test.com\r\n", 14);
    EXPECT_EQ(Decoder::Result::ReadyForNext, decoder_impl_->parseCommand(*data_));
    EXPECT_EQ(SmtpSession::State::SESSION_INIT_REQUEST, decoder_impl_->getSession().getState());
}

TEST_F(DecoderImplTest, TestParseCommandStartTls) {
  data_->add("STARTTLS\r\n", 10);
  // Test case when session is already encrypted
  decoder_impl_->getSession().setSessionEncrypted(true);
  decoder_impl_->getSession().setState(SmtpSession::State::SESSION_IN_PROGRESS);
  EXPECT_CALL(callbacks_, sendReplyDownstream(SmtpUtils::outOfOrderCommandResponse));
  EXPECT_EQ(decoder_impl_->parseCommand(*data_), Decoder::Result::Stopped);
  
  // Test case when session is not encrypted, and client sends STARTTLS, encryption is successful
  std::cout << "data: " << data_->toString() << "\n";
  decoder_impl_->getSession().setSessionEncrypted(false);
  EXPECT_CALL(callbacks_, upstreamTlsRequired()).WillOnce(testing::Return(false));
  EXPECT_CALL(callbacks_, downstreamStartTls(SmtpUtils::readyToStartTlsResponse)).WillOnce(testing::Return(false));
  EXPECT_EQ(decoder_impl_->parseCommand(*data_), Decoder::Result::Stopped);
  EXPECT_EQ(decoder_impl_->getSession().isSessionEncrypted(), true);

  // Test case when callback returns false for downstreamStartTls, encryption error
  decoder_impl_->getSession().setSessionEncrypted(false);
  EXPECT_CALL(callbacks_, upstreamTlsRequired()).WillOnce(testing::Return(false));
  EXPECT_CALL(callbacks_, downstreamStartTls(SmtpUtils::readyToStartTlsResponse)).WillOnce(testing::Return(true));
  EXPECT_CALL(callbacks_, incTlsTerminationErrors());
//   EXPECT_CALL(callbacks_, sendReplyDownstream("454 4.7.0 TLS not available due to temporary reason"));
  EXPECT_CALL(callbacks_, sendReplyDownstream(SmtpUtils::tlsHandshakeErrorResponse));
  EXPECT_EQ(decoder_impl_->parseCommand(*data_), Decoder::Result::Stopped);
}

TEST(DecoderImplTest, TestSmtpTransactionCommands) {
  DecoderImpl decoder;
  SmtpSession session;
  
  // Test case 1: NONE or TRANSACTION_COMPLETED state and smtpMailCommand
  std::string command = SmtpUtils::smtpMailCommand;
  session.SetTransactionState(SmtpTransaction::State::NONE);
  decoder.decodeSmtpTransactionCommands(command);
  EXPECT_EQ(session.getTransactionState(), SmtpTransaction::State::TRANSACTION_REQUEST);
  
  session.SetTransactionState(SmtpTransaction::State::TRANSACTION_COMPLETED);
  decoder.decodeSmtpTransactionCommands(command);
  EXPECT_EQ(session.getTransactionState(), SmtpTransaction::State::TRANSACTION_REQUEST);
  
  // Test case 2: RCPT_COMMAND, MAIL_DATA_TRANSFER_REQUEST, or TRANSACTION_IN_PROGRESS state and smtpRcptCommand
  command = SmtpUtils::smtpRcptCommand;
  session.SetTransactionState(SmtpTransaction::State::RCPT_COMMAND);
  decoder.decodeSmtpTransactionCommands(command);
  EXPECT_EQ(session.getTransactionState(), SmtpTransaction::State::RCPT_COMMAND);
  
  session.SetTransactionState(SmtpTransaction::State::MAIL_DATA_TRANSFER_REQUEST);
  decoder.decodeSmtpTransactionCommands(command);
  EXPECT_EQ(session.getTransactionState(), SmtpTransaction::State::RCPT_COMMAND);
  
  session.SetTransactionState(SmtpTransaction::State::TRANSACTION_IN_PROGRESS);
  decoder.decodeSmtpTransactionCommands(command);
  EXPECT_EQ(session.getTransactionState(), SmtpTransaction::State::RCPT_COMMAND);
  
  // Test case 3: RCPT_COMMAND, MAIL_DATA_TRANSFER_REQUEST, or TRANSACTION_IN_PROGRESS state and smtpDataCommand
  command = SmtpUtils::smtpDataCommand;
  session.SetTransactionState(SmtpTransaction::State::RCPT_COMMAND);
  decoder.decodeSmtpTransactionCommands(command);
  EXPECT_EQ(session.getTransactionState(), SmtpTransaction::State::MAIL_DATA_TRANSFER_REQUEST);
  
  session.SetTransactionState(SmtpTransaction::State::MAIL_DATA_TRANSFER_REQUEST);
  decoder.decodeSmtpTransactionCommands(command);
  EXPECT_EQ(session.getTransactionState(), SmtpTransaction::State::MAIL_DATA_TRANSFER_REQUEST);
  
  session.SetTransactionState(SmtpTransaction::State::TRANSACTION_IN_PROGRESS);
  decoder.decodeSmtpTransactionCommands(command);
//   EXPECT_EQ(session

}

TEST(DecoderImplTest, TestSmtpMailCommand) {
  DecoderImpl decoder;
  SmtpSession session;
  std::string command = "MAIL FROM:<test@example.com>";
  decoder.session_ = session;

  decoder.decodeSmtpTransactionCommands(command);
  EXPECT_EQ(decoder.session_.getTransactionState(), SmtpTransaction::State::TRANSACTION_REQUEST);
}

TEST(DecoderImplTest, TestSmtpRcptCommand) {
  DecoderImpl decoder;
  SmtpSession session;
  session.SetTransactionState(SmtpTransaction::State::TRANSACTION_IN_PROGRESS);
  std::string command = "RCPT TO:<test@example.com>";
  decoder.session_ = session;

  decoder.decodeSmtpTransactionCommands(command);
  EXPECT_EQ(decoder.session_.getTransactionState(), SmtpTransaction::State::RCPT_COMMAND);
}

TEST(DecoderImplTest, TestSmtpDataCommand) {
  DecoderImpl decoder;
  SmtpSession session;
  session.SetTransactionState(SmtpTransaction::State::TRANSACTION_IN_PROGRESS);
  std::string command = "DATA";
  decoder.session_ = session;

  decoder.decodeSmtpTransactionCommands(command);
  EXPECT_EQ(decoder.session_.getTransactionState(), SmtpTransaction::State::MAIL_DATA_TRANSFER_REQUEST);
}

TEST(DecoderImplTest, TestSmtpRsetCommand) {
  DecoderImpl decoder;
  SmtpSession session;
  session.SetTransactionState(SmtpTransaction::State::TRANSACTION_IN_PROGRESS);
  std::string command = "RSET";
  decoder.session_ = session;

  decoder.decodeSmtpTransactionCommands(command);
  EXPECT_EQ(decoder.session_.getTransactionState(), SmtpTransaction::State::TRANSACTION_ABORT_REQUEST);
}

TEST(DecoderImplTest, TestSmtpEhloCommand) {
  DecoderImpl decoder;
  SmtpSession session;
  session.SetTransactionState(SmtpTransaction::State::TRANSACTION_IN_PROGRESS);
  std::string command = "EHLO";
  decoder.session_ = session;

  decoder.decodeSmtpTransactionCommands(command);
  EXPECT_EQ(decoder.session_.getTransactionState(), SmtpTransaction::State::TRANSACTION_ABORT_REQUEST);
}


TEST_F(ParseCommandTest, TestCommandTooSmall) {
  Buffer::OwnedImpl data;
  data.add("command");
  EXPECT_EQ(decoder_.parseCommand(data), Decoder::Result::ReadyForNext);
}

TEST_F(ParseCommandTest, TestConnectionSuccess) {
  session_.setState(SmtpSession::State::CONNECTION_SUCCESS);
  Buffer::OwnedImpl data;
  data.add("EHLO");
  EXPECT_EQ(decoder_.parseCommand(data), Decoder::Result::ReadyForNext);
  EXPECT_EQ(session_.getState(), SmtpSession::State::SESSION_INIT_REQUEST);

  data.drain(data.length());
  data.add("HELO");
  EXPECT_EQ(decoder_.parseCommand(data), Decoder::Result::ReadyForNext);
  EXPECT_EQ(session_.getState(), SmtpSession::State::SESSION_INIT_REQUEST);
}

TEST_F(ParseCommandTest, TestUpstreamTlsNegotiation) {
  session_.setState(SmtpSession::State::UPSTREAM_TLS_NEGOTIATION);
  EXPECT_CALL(callbacks_, sendReplyDownstream(SmtpUtils::mailboxUnavailableResponse));
  Buffer::OwnedImpl data;
  data.add("EHLO");
  EXPECT_EQ(decoder_.parseCommand(data), Decoder::Result::Stopped);
}

TEST_F(ParseCommandTest, TestSessionInProgressStartTls) {
  session_.setState(SmtpSession::State::SESSION_IN_PROGRESS);
  Buffer::OwnedImpl data;
  data.add("STARTTLS");
  EXPECT_CALL(callbacks_, upstreamTlsRequired()).WillOnce(testing::Return(false));
  EXPECT_CALL(callbacks_, handleDownstreamTls());
  EXPECT_EQ(decoder_.parseCommand(data), Decoder::Result::Stopped);

  testing::Mock::VerifyAndClearExpectations(&callbacks_);

  data.drain(data.length());
  data.add("STARTTLS params");
  EXPECT_CALL(callbacks_, sendReplyDownstream(SmtpUtils::syntaxErrorNoParamsAllowed));
  EXPECT_EQ(decoder_.parseCommand(data), Decoder::Result::Stopped);

  testing::Mock::VerifyAndClearExpectations(&callbacks_);

}

TEST_F(DecoderImplTest, ParseCommand_SessionInProgress_InvalidCommand) {
  // Test parseCommand function when the session state is SESSION_IN_PROGRESS and an invalid command is received
  data_->add("invalid command\r\n");
  decoder_impl_->session_.setState(SmtpSession::State::SESSION_IN_PROGRESS);
  EXPECT_EQ(Decoder::Result::ReadyForNext, decoder_impl_->parseCommand(*data_));
  EXPECT_EQ(SmtpSession::State::SESSION_IN_PROGRESS, decoder_impl_->session_.getState());
}

TEST_F(DecoderImplTest, ParseCommand_SessionInProgress_QuitCommand) {
  // Test parseCommand function when the session state is SESSION_IN_PROGRESS and the QUIT command is received
  data_->add("quit\r\n");
  decoder_impl_->session_.setState(SmtpSession::State::SESSION_IN_PROGRESS);
  EXPECT_EQ(Decoder::Result::ReadyForNext, decoder_impl_->parseCommand(*data_));
  EXPECT_EQ(SmtpSession::State::SESSION_TERMINATION_REQUEST, decoder_impl_->session_.getState());
}

TEST_F(DecoderImplTest, parseResponse_ConnectionSuccess) {
  // Set the initial state of the session to CONNECTION_REQUEST.
  session_.setState(SmtpSession::State::CONNECTION_REQUEST);

  // Prepare the response data.
  data_->add("220");

  // Call the parseResponse function.
  decoder_impl_->parseResponse(*data_);

  // Check that the session state was correctly updated.
  EXPECT_EQ(session_.getState(), SmtpSession::State::CONNECTION_SUCCESS);
}


TEST_F(DecoderImplTest, parseResponse_ConnectionEstablishmentError) {
  // Set the initial state of the session to CONNECTION_REQUEST.
  session_.setState(SmtpSession::State::CONNECTION_REQUEST);

  // Prepare the response data.
  data_->add("554");

  // Call the parseResponse function.
  decoder_impl_->parseResponse(*data_);

  // Check that the "smtp_connection_establishment_errors" stat was correctly incremented.
  EXPECT_CALL(callbacks_, incSmtpConnectionEstablishmentErrors()).Times(1);
}


TEST_F(DecoderImplTest, parseResponse_SessionInProgress) {
  // Set the initial state of the session to SESSION_INIT_REQUEST.
  session_.setState(SmtpSession::State::SESSION_INIT_REQUEST);

  // Prepare the response data.
  data_->add("250");

  // Call the parseResponse function.
  decoder_impl_->parseResponse(*data_);

  // Check that the session state was correctly updated.
  EXPECT_EQ(session_.getState(), SmtpSession::State::SESSION_IN_PROGRESS);
}


TEST_F(DecoderImplTest, parseResponse_AuthError) {
  // Set the initial state of the session to SESSION_AUTH_REQUEST.
  session_.setState(SmtpSession::State::SESSION_AUTH_REQUEST);

  // Prepare the response data.
  data_->add("500");

  // Call the parseResponse function.
  decoder_impl_->parseResponse(*data_);

  // Check that the "smtp_auth_errors" stat was correctly incremented.
  EXPECT_CALL(callbacks_, incSmtpAuthErrors()).Times(1);
}

TEST_F(DecoderImplTest, TestParseResponseForUPSTREAM_TLS_NEGOTIATION) {
  // Set session state to UPSTREAM_TLS_NEGOTIATION
  session_.SetSessionState(SmtpSession::State::UPSTREAM_TLS_NEGOTIATION);
  uint16_t response_code = 220;

  // Test the case where the response code is 220 (Service ready)
  EXPECT_CALL(callbacks_, incUpstreamTlsSuccess());
  EXPECT_CALL(callbacks_, upstreamStartTls());
  decoder_->parseResponse(response_code);
  EXPECT_EQ(SmtpSession::State::UPSTREAM_TLS_ESTABLISHED, session_.getSessionState());

  // Reset session state to UPSTREAM_TLS_NEGOTIATION
  session_.SetSessionState(SmtpSession::State::UPSTREAM_TLS_NEGOTIATION);
  response_code = 554;

  // Test the case where the response code is 554 (Transaction failed)
  EXPECT_CALL(callbacks_, incUpstreamTlsFailed());
  decoder_->parseResponse(response_code);
  EXPECT_EQ(SmtpSession::State::UPSTREAM_TLS_NEGOTIATION, session_.getSessionState());
}


TEST_F(DecoderImplTest, TestParseResponseForUPSTREAM_TLS_NEGOTIATION_220Response) {
  SmtpTransaction transaction = SmtpTransaction::State::UPSTREAM_TLS_NEGOTIATION;
  session_.setTransactionState(transaction);

  // 220 is the response for the "STARTTLS" command
  uint16_t response_code = 220;
  decoder_->parseResponse(response_code);
  
  // Expect the state to remain unchanged
  EXPECT_EQ(session_.getTransactionState(), SmtpTransaction::State::UPSTREAM_TLS_NEGOTIATION);
  // Expect the callback function incUpstreamTlsSuccess to be called
  EXPECT_CALL(*callbacks_, incUpstreamTlsSuccess());
  // Verify that the mock object has received the expected call
  Mock::VerifyAndClearExpectations(callbacks_);
}

TEST_F(DecoderImplTest, TestParseResponseForUPSTREAM_TLS_NEGOTIATION_UnsuccessfulResponse) {
  SmtpTransaction transaction = SmtpTransaction::State::UPSTREAM_TLS_NEGOTIATION;
  session_.setTransactionState(transaction);

  // A response code other than 220 will be considered as an unsuccessful response
  uint16_t response_code = 500;
  decoder_->parseResponse(response_code);
  
  // Expect the state to change to NONE
  EXPECT_EQ(session_.getTransactionState(), SmtpTransaction::State::NONE);
  // Expect the callback function incUpstreamTlsFailed to be called
  EXPECT_CALL(*callbacks_, incUpstreamTlsFailed());
  // Verify that the mock object has received the expected call
  Mock::VerifyAndClearExpectations(callbacks_);
}


TEST_F(DecoderImplTest, DecodeSmtpTransactionResponse_InTransactionRequestState) {
  uint16_t response_code = 250;
  session_.SetTransactionState(SmtpTransaction::State::TRANSACTION_REQUEST);
  
  decoder_.decodeSmtpTransactionResponse(response_code);

  EXPECT_EQ(session_.getTransactionState(), SmtpTransaction::State::TRANSACTION_IN_PROGRESS);
}

TEST_F(DecoderImplTest, DecodeSmtpTransactionResponse_InRcptCommandState_ResponseCode250) {
  uint16_t response_code = 250;
  session_.SetTransactionState(SmtpTransaction::State::RCPT_COMMAND);
  
  decoder_.decodeSmtpTransactionResponse(response_code);

  EXPECT_EQ(session_.getTransactionState(), SmtpTransaction::State::TRANSACTION_IN_PROGRESS);
}

TEST_F(DecoderImplTest, DecodeSmtpTransactionResponse_InRcptCommandState_ResponseCode251) {
  uint16_t response_code = 251;
  session_.SetTransactionState(SmtpTransaction::State::RCPT_COMMAND);
  
  decoder_.decodeSmtpTransactionResponse(response_code);

  EXPECT_EQ(session_.getTransactionState(), SmtpTransaction::State::TRANSACTION_IN_PROGRESS);
}

TEST_F(DecoderImplTest, DecodeSmtpTransactionResponse_InRcptCommandState_ResponseCode400) {
  uint16_t response_code = 400;
  session_.SetTransactionState(SmtpTransaction::State::RCPT_COMMAND);
  
  decoder_.decodeSmtpTransactionResponse(response_code);

  EXPECT_EQ(session_.getTransactionState(), SmtpTransaction::State::RCPT_COMMAND);
  EXPECT_CALL(*callbacks_, incMailRcptErrors());
}

TEST_F(DecoderImplTest, DecodeSmtpTransactionResponse_InMailDataTransferRequestState_ResponseCode250) {
  uint16_t response_code = 250;
  session_.SetTransactionState(SmtpTransaction::State::MAIL_DATA_TRANSFER_REQUEST);
  
  decoder_.decodeSmtpTransactionResponse(response_code);

  EXPECT_EQ(session_.getTransactionState(), SmtpTransaction::State::TRANSACTION_COMPLETED);
  EXPECT_CALL(*callbacks_, incSmtpTransactions());
}

TEST_F(DecoderImplTest, DecodeSmtpTransactionResponse_InMailDataTransferRequestState_ResponseCode400) {
  uint16_t response_code = 400;
  session_.SetTransactionState(SmtpTransaction::State::MAIL_DATA_TRANSFER_REQUEST);
  
  decoder_.decodeSmtpTransactionResponse(response_code);

  EXPECT_EQ(session_.getTransactionState(), SmtpTransaction::State::NONE);
  EXPECT_CALL(*callbacks_, incMailDataTransferErrors());
//   EXPECT_CALL(*callbacks_, incSmtpTransactions


}

TEST_F(DecoderImplTest, TestDecodeSmtpTransactionResponseForTRANSACTION_ABORT_REQUEST_ValidResponse) {
// Setup
uint16_t response_code = 250;
session_.SetTransactionState(SmtpTransaction::State::TRANSACTION_ABORT_REQUEST);

// Act
decoder_impl_->decodeSmtpTransactionResponse(response_code);

// Assert
EXPECT_CALL(callbacks_, incSmtpTransactionsAborted()).Times(1);
EXPECT_EQ(SmtpTransaction::State::NONE, session_.getTransactionState());
}

} // namespace SmtpProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
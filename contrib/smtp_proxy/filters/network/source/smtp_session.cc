#include "contrib/smtp_proxy/filters/network/source/smtp_session.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/stats/timespan_impl.h"
#include "source/extensions/filters/network/well_known_names.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace SmtpProxy {

SmtpSession::SmtpSession(DecoderCallbacks* callbacks, TimeSource& time_source,
                         Random::RandomGenerator& random_generator)
    : callbacks_(callbacks), time_source_(time_source), random_generator_(random_generator) {
  newCommand(StringUtil::toUpper("connect_req"), SmtpCommand::Type::NonTransactionCommand);
  session_length_ = std::make_unique<Stats::HistogramCompletableTimespanImpl>(
      callbacks_->getStats().smtp_session_length_, time_source_);
  callbacks_->incActiveSession();
}

void SmtpSession::newCommand(const std::string& name, SmtpCommand::Type type) {
  current_command_ = std::make_shared<SmtpCommand>(name, type, time_source_);
  command_in_progress_ = true;
  command_length_ = std::make_unique<Stats::HistogramCompletableTimespanImpl>(
      callbacks_->getStats().smtp_command_length_, time_source_);
}

void SmtpSession::createNewTransaction() {
  if (smtp_transaction_ == nullptr) {
    smtp_transaction_ =
        new SmtpTransaction(session_id_, callbacks_, time_source_, random_generator_);
    transaction_in_progress_ = true;
  }
}

void SmtpSession::updateBytesMeterOnCommand(Buffer::Instance& data) {
  if (data.length() == 0)
    return;

  if (transaction_in_progress_) {
    getTransaction()->getStreamInfo().addBytesReceived(data.length());
    getTransaction()->getStreamInfo().getDownstreamBytesMeter()->addWireBytesReceived(
        data.length());
    getTransaction()->getStreamInfo().getUpstreamBytesMeter()->addHeaderBytesSent(data.length());

    if (isDataTransferInProgress()) {
      getTransaction()->addPayloadBytes(data.length());
    }
  }
}

void SmtpSession::updateBytesMeterOnResponse(Buffer::Instance& data) {
  if (data.length() == 0)
    return;

  if (transaction_in_progress_) {
    getTransaction()->getStreamInfo().addBytesSent(data.length());
    getTransaction()->getStreamInfo().getDownstreamBytesMeter()->addWireBytesSent(data.length());
    getTransaction()->getStreamInfo().getUpstreamBytesMeter()->addWireBytesReceived(data.length());
  }
}

SmtpUtils::Result SmtpSession::handleCommand(std::string& command, std::string& args) {

  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;

  if (command == "")
    return result;

  if (absl::EqualsIgnoreCase(command, SmtpUtils::smtpEhloCommand) ||
      absl::EqualsIgnoreCase(command, SmtpUtils::smtpHeloCommand)) {
    result = handleEhlo(command);
  } else if (absl::EqualsIgnoreCase(command, SmtpUtils::smtpMailCommand)) {
    result = handleMail(args);
  } else if (absl::EqualsIgnoreCase(command, SmtpUtils::smtpRcptCommand)) {
    result = handleRcpt(args);
  } else if (absl::EqualsIgnoreCase(command, SmtpUtils::smtpDataCommand)) {
    result = handleData(args);
  } else if (absl::EqualsIgnoreCase(command, SmtpUtils::smtpAuthCommand)) {
    result = handleAuth();
  } else if (absl::EqualsIgnoreCase(command, SmtpUtils::startTlsCommand)) {
    result = handleStarttls();
  } else if (absl::EqualsIgnoreCase(command, SmtpUtils::smtpRsetCommand)) {
    result = handleReset(args);
  } else if (absl::EqualsIgnoreCase(command, SmtpUtils::smtpQuitCommand)) {
    result = handleQuit(args);
  } else {
    result = handleOtherCmds(command);
  }
  return result;
}

SmtpUtils::Result SmtpSession::handleEhlo(std::string& command) {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;

  newCommand(StringUtil::toUpper(command), SmtpCommand::Type::NonTransactionCommand);
  setState(SmtpSession::State::SessionInitRequest);
  return result;
}

SmtpUtils::Result SmtpSession::handleMail(std::string& arg) {

  if(transaction_in_progress_) {
    getTransaction()->setStatus(SmtpUtils::passthroughMode);
    return SmtpUtils::Result::ProtocolError;
  }

  newCommand(SmtpUtils::smtpMailCommand, SmtpCommand::Type::TransactionCommand);
  if (state_ != SmtpSession::State::SessionInProgress) {
    current_command_->storeLocalResponse("Please introduce yourself first with EHLO/HELO", "local",
                                         502);
    callbacks_->sendReplyDownstream(SmtpUtils::generateResponse(
        502, {5, 5, 1}, "Please introduce yourself first with EHLO/HELO"));
    return SmtpUtils::Result::Stopped;
  }

  if (arg.length() < 6 || !absl::EqualsIgnoreCase(arg.substr(0, 5), "FROM:")) {
    // MAIL FROM command not in proper syntax, enter into passthrough mode.
    storeResponse("Protocol Error", "local", 0, SmtpCommand::ResponseType::Local);
    return SmtpUtils::Result::ProtocolError;
  }

  // If STARTTLS is required for this session and client has not issued it earlier, abort the
  // session.
  if (callbacks_->downstreamTlsRequired() && !isSessionEncrypted()) {
    current_command_->storeLocalResponse("plain-text connection is not allowed for this server. Please upgrade the connection to TLS", "local", 523);
    callbacks_->sendReplyDownstream(
        SmtpUtils::generateResponse(523, {5, 7, 10}, "plain-text connection is not allowed for this server. Please upgrade the connection to TLS"));
    terminateSession(SmtpUtils::statusError, "plain-text connection is not allowed for this server. Please upgrade the connection to TLS");
    callbacks_->closeDownstreamConnection();
    return SmtpUtils::Result::Stopped;
  }

  createNewTransaction();
  std::string sender = SmtpUtils::extractAddress(arg);
  getTransaction()->setSender(sender);
  callbacks_->incSmtpTransactionRequests();
  updateBytesMeterOnCommand(callbacks_->getReadBuffer());
  setTransactionState(SmtpTransaction::State::TransactionRequest);
  return SmtpUtils::Result::ReadyForNext;
}

SmtpUtils::Result SmtpSession::handleRcpt(std::string& arg) {
  newCommand(SmtpUtils::smtpRcptCommand, SmtpCommand::Type::TransactionCommand);

  if (!transaction_in_progress_) {
    current_command_->storeLocalResponse("Missing MAIL FROM command", "local", 502);
    callbacks_->sendReplyDownstream(
        SmtpUtils::generateResponse(502, {5, 5, 1}, "Missing MAIL FROM command"));
    return SmtpUtils::Result::Stopped;
  }

  if (arg.length() <= 3 || !absl::EqualsIgnoreCase(arg.substr(0, 3), "TO:")) {
    storeResponse("Protocol Error", "local", 0, SmtpCommand::ResponseType::Local);
    return SmtpUtils::Result::ProtocolError;
  }

  std::string recipient = SmtpUtils::extractAddress(arg);
  getTransaction()->addRcpt(recipient);
  updateBytesMeterOnCommand(callbacks_->getReadBuffer());
  setTransactionState(SmtpTransaction::State::RcptCommand);
  return SmtpUtils::Result::ReadyForNext;
}

SmtpUtils::Result SmtpSession::handleData(std::string& arg) {
  newCommand(SmtpUtils::smtpDataCommand, SmtpCommand::Type::TransactionCommand);
  if (arg.length() > 0) {
    current_command_->storeLocalResponse("No params allowed for DATA command", "local", 501);
    callbacks_->sendReplyDownstream(
        SmtpUtils::generateResponse(501, {5, 5, 4}, "No params allowed for DATA command"));
    return SmtpUtils::Result::Stopped;
  }

  if (!transaction_in_progress_ || getTransaction()->getNoOfRecipients() == 0) {
    current_command_->storeLocalResponse("Missing RCPT TO command", "local", 502);
    callbacks_->sendReplyDownstream(
        SmtpUtils::generateResponse(502, {5, 5, 1}, "Missing RCPT TO command"));
    return SmtpUtils::Result::Stopped;
  }

  updateBytesMeterOnCommand(callbacks_->getReadBuffer());
  setTransactionState(SmtpTransaction::State::MailDataTransferRequest);
  return SmtpUtils::Result::ReadyForNext;
}

SmtpUtils::Result SmtpSession::handleAuth() {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;
  newCommand(SmtpUtils::smtpAuthCommand, SmtpCommand::Type::NonTransactionCommand);
  if (isAuthenticated()) {
    current_command_->storeLocalResponse("Already authenticated", "local", 502);
    callbacks_->sendReplyDownstream(
        SmtpUtils::generateResponse(502, {5, 5, 1}, "Already authenticated"));
    result = SmtpUtils::Result::Stopped;
    return result;
  }
  setState(SmtpSession::State::SessionAuthRequest);
  return result;
}

SmtpUtils::Result SmtpSession::handleStarttls() {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;

  newCommand(SmtpUtils::startTlsCommand, SmtpCommand::Type::NonTransactionCommand);
  if (isSessionEncrypted()) {
    // record local response
    current_command_->storeLocalResponse(SmtpUtils::tlsSessionActiveAlready, "local", 502);
    callbacks_->sendReplyDownstream(
        SmtpUtils::generateResponse(502, {5, 5, 1}, SmtpUtils::tlsSessionActiveAlready));
    result = SmtpUtils::Result::Stopped;
    return result;
  }

  if (callbacks_->downstreamTlsEnabled()) {
    if (callbacks_->upstreamTlsEnabled()) {
      // Forward STARTTLS request to upstream for TLS termination at upstream end.
      setState(SmtpSession::State::UpstreamTlsNegotiation);
    } else {
      // Perform downstream TLS termination.
      handleDownstreamTls();
      result = SmtpUtils::Result::Stopped;
    }
    return result;
  }
  // TODO below: handle case when client <--- plaintext ---> envoy <--- TLS ---> upstream ?

  if (callbacks_->upstreamTlsEnabled()) {
    // Forward STARTTLS request to upstream for TLS termination at upstream.
    setState(SmtpSession::State::UpstreamTlsNegotiation);
  }
  return result;
}

SmtpUtils::Result SmtpSession::handleReset(std::string& arg) {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;

  newCommand(SmtpUtils::smtpRsetCommand, SmtpCommand::Type::NonTransactionCommand);
  if (arg.length() > 0) {
    current_command_->storeLocalResponse("No params allowed for RSET command", "local", 501);
    callbacks_->sendReplyDownstream(
        SmtpUtils::generateResponse(501, {5, 5, 4}, "No params allowed for RSET command"));
    result = SmtpUtils::Result::Stopped;
    return result;
  }

  if (getTransaction() != nullptr) {
    setTransactionState(SmtpTransaction::State::TransactionAbortRequest);
  }
  return result;
}

SmtpUtils::Result SmtpSession::handleQuit(std::string& arg) {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;

  newCommand(SmtpUtils::smtpQuitCommand, SmtpCommand::Type::NonTransactionCommand);
  if (arg.length() > 0) {
    current_command_->storeLocalResponse("No params allowed for QUIT command", "local", 501);
    callbacks_->sendReplyDownstream(
        SmtpUtils::generateResponse(501, {5, 5, 4}, "No params allowed for QUIT command"));
    result = SmtpUtils::Result::Stopped;
    return result;
  }
  setState(SmtpSession::State::SessionTerminationRequest);
  if (getTransaction() != nullptr) {
    setTransactionState(SmtpTransaction::State::TransactionAbortRequest);
  }
  return result;
}

SmtpUtils::Result SmtpSession::handleOtherCmds(std::string& command) {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;
  newCommand(StringUtil::toUpper(command), SmtpCommand::Type::NonTransactionCommand);
  return result;
}

SmtpUtils::Result SmtpSession::handleResponse(int& response_code, std::string& response) {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;

  // Special handling to parse any error response to connection request.
  if (getState() == SmtpSession::State::ConnectionRequest) {
    return handleConnResponse(response_code, response);
  }
  if (!isCommandInProgress()) {
    return result;
  }
  std::string command = getCurrentCommand()->getName();

  if (absl::EqualsIgnoreCase(command, SmtpUtils::smtpEhloCommand) ||
      absl::EqualsIgnoreCase(command, SmtpUtils::smtpHeloCommand)) {
    result = handleEhloResponse(response_code, response);
  } else if (absl::EqualsIgnoreCase(command, SmtpUtils::smtpMailCommand)) {
    result = handleMailResponse(response_code, response);
  } else if (absl::EqualsIgnoreCase(command, SmtpUtils::smtpRcptCommand)) {
    result = handleRcptResponse(response_code, response);
  } else if (absl::EqualsIgnoreCase(command, SmtpUtils::smtpDataCommand)) {
    result = handleDataResponse(response_code, response);
  } else if (absl::EqualsIgnoreCase(command, SmtpUtils::smtpAuthCommand)) {
    result = handleAuthResponse(response_code, response);
  } else if (absl::EqualsIgnoreCase(command, SmtpUtils::startTlsCommand)) {
    result = handleStarttlsResponse(response_code, response);
  } else if (absl::EqualsIgnoreCase(command, SmtpUtils::smtpRsetCommand)) {
    result = handleResetResponse(response_code, response);
  } else if (absl::EqualsIgnoreCase(command, SmtpUtils::smtpQuitCommand)) {
    result = handleQuitResponse(response_code, response);
  } else if (absl::EqualsIgnoreCase(command, SmtpUtils::xReqIdCommand)) {
    result = handleXReqIdResponse(response_code, response);
  } else {
    result = handleOtherResponse(response_code, response);
  }

  return result;
}

SmtpUtils::Result SmtpSession::handleConnResponse(int& response_code, std::string& response) {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;
  storeResponse(response, SmtpUtils::via_upstream, response_code,
                SmtpCommand::ResponseType::ViaUpstream);
  if (response_code == 220) {
    setState(SmtpSession::State::ConnectionSuccess);
    auto stream_id_provider = callbacks_->getStreamInfo().getStreamIdProvider();
    if (stream_id_provider.has_value()) {
      session_id_ = stream_id_provider->toStringView().value_or("");
    }

    if (callbacks_->tracingEnabled() && !isXReqIdSent()) {
      response_on_hold_ =
          std::to_string(response_code) + " " + response + SmtpUtils::smtpCrlfSuffix;
      std::string x_req_id =
          std::string(SmtpUtils::xReqIdCommand) + " SESSION_ID=" + session_id_ + "\r\n";
      Buffer::OwnedImpl data(x_req_id);
      newCommand(SmtpUtils::xReqIdCommand, SmtpCommand::Type::NonTransactionCommand);
      callbacks_->sendUpstream(data);
      setState(SmtpSession::State::XReqIdTransfer);
      result = SmtpUtils::Result::Stopped;
      return result;
    }
  } else if (response_code >= 400 && response_code <= 599) {
    callbacks_->getStats().smtp_connection_establishment_errors_.inc();
    connect_resp_.response_code = response_code;
    connect_resp_.response_code_details = SmtpUtils::via_upstream;
    connect_resp_.response_str = response;
    setStatus(SmtpUtils::statusFailed);
    msg_ = "Connection Establishment Error";
  }
  return result;
}

SmtpUtils::Result SmtpSession::handleEhloResponse(int& response_code, std::string& response) {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;
  if (response_code == 250) {
    setState(SmtpSession::State::SessionInProgress);
    if (transaction_in_progress_) {
      // Abort this transaction.
      abortTransaction(SmtpUtils::resetDueToEhlo);
    }
  } else {
    setState(SmtpSession::State::ConnectionSuccess);
  }

  storeResponse(response, SmtpUtils::via_upstream, response_code,
                SmtpCommand::ResponseType::ViaUpstream);
  return result;
}

SmtpUtils::Result SmtpSession::handleMailResponse(int& response_code, std::string& response) {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;
  storeResponse(response, SmtpUtils::via_upstream, response_code,
                SmtpCommand::ResponseType::ViaUpstream);
  updateBytesMeterOnResponse(callbacks_->getWriteBuffer());
  if (response_code == 250) {
    setTransactionState(SmtpTransaction::State::TransactionInProgress);
    callbacks_->incActiveTransaction();
    if (callbacks_->tracingEnabled() && !getTransaction()->isXReqIdSent()) {
      response_on_hold_ =
          std::to_string(response_code) + " " + response + SmtpUtils::smtpCrlfSuffix;
      std::string x_req_id = std::string(SmtpUtils::xReqIdCommand) +
                             " TRXN_ID=" + getTransaction()->getTransactionId() + "\r\n";
      Buffer::OwnedImpl data(x_req_id);
      newCommand(SmtpUtils::xReqIdCommand, SmtpCommand::Type::NonTransactionCommand);
      updateBytesMeterOnCommand(data);
      callbacks_->sendUpstream(data);
      setTransactionState(SmtpTransaction::State::XReqIdTransfer);
      result = SmtpUtils::Result::Stopped;
      return result;
    }
  } else {
    // error response to mail command
    getTransaction()->setMsg(response);
    getTransaction()->setStatus(SmtpUtils::statusError);
    setTransactionState(SmtpTransaction::State::None);
  }

  return result;
}

SmtpUtils::Result SmtpSession::handleXReqIdResponse(int& response_code, std::string& response) {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;
  storeResponse(response, SmtpUtils::via_upstream, response_code,
                SmtpCommand::ResponseType::ViaUpstream);
  if (transaction_in_progress_) {
    // We sent trxn level x_req_id
    updateBytesMeterOnResponse(callbacks_->getWriteBuffer());
    setTransactionState(SmtpTransaction::State::TransactionInProgress);
    getTransaction()->setXReqIdSent(true);
  } else {
    // We sent session level x_req_id
    x_req_id_sent_ = true;
    setState(SmtpSession::State::ConnectionSuccess);
  }

  callbacks_->sendReplyDownstream(getResponseOnHold());
  result = SmtpUtils::Result::Stopped;
  return result;
}

SmtpUtils::Result SmtpSession::handleRcptResponse(int& response_code, std::string& response) {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;

  if (response_code == 250 || response_code == 251) {
    setTransactionState(SmtpTransaction::State::TransactionInProgress);
  } else if (response_code >= 400 && response_code <= 599) {
    getTransaction()->setMsg(response);
    getTransaction()->setStatus(SmtpUtils::statusError);
    callbacks_->incMailRcptErrors();
  }
  storeResponse(response, SmtpUtils::via_upstream, response_code,
                SmtpCommand::ResponseType::ViaUpstream);
  updateBytesMeterOnResponse(callbacks_->getWriteBuffer());
  return result;
}

SmtpUtils::Result SmtpSession::handleDataResponse(int& response_code, std::string& response) {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;
  storeResponse(response, SmtpUtils::via_upstream, response_code,
                SmtpCommand::ResponseType::ViaUpstream);
  updateBytesMeterOnResponse(callbacks_->getWriteBuffer());
  if (response_code == 250) {
    setTransactionState(SmtpTransaction::State::TransactionCompleted);
    getTransaction()->setStatus(SmtpUtils::statusSuccess);
    onTransactionComplete();
  } else if (response_code == 354) {
    // Intermediate response.
    setDataTransferInProgress(true);
    data_tx_length_ = std::make_unique<Stats::HistogramCompletableTimespanImpl>(
        callbacks_->getStats().smtp_data_transfer_length_, time_source_);

    return result;
  } else if (response_code >= 400 && response_code <= 599) {
    callbacks_->incMailDataTransferErrors();
    setTransactionState(SmtpTransaction::State::None);
    onTransactionFailed(response);
  }
  if (data_tx_length_ != nullptr) {
    data_tx_length_->complete();
  }
  setDataTransferInProgress(false);
  return result;
}

SmtpUtils::Result SmtpSession::handleResetResponse(int& response_code, std::string& response) {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;
  storeResponse(response, SmtpUtils::via_upstream, response_code,
                SmtpCommand::ResponseType::ViaUpstream);

  if (transaction_in_progress_) {
    if (response_code == 250) {
      abortTransaction(SmtpUtils::resetDueToRset);
    } else {
      setTransactionState(SmtpTransaction::State::TransactionInProgress);
    }
  }

  return result;
}

SmtpUtils::Result SmtpSession::handleQuitResponse(int& response_code, std::string& response) {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;
  storeResponse(response, SmtpUtils::via_upstream, response_code,
                SmtpCommand::ResponseType::ViaUpstream);
  if (response_code == 221) {
    onSessionComplete();
  } else {
    setState(SmtpSession::State::SessionInProgress);
  }
  return result;
}

SmtpUtils::Result SmtpSession::handleAuthResponse(int& response_code, std::string& response) {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;

  storeResponse(response, SmtpUtils::via_upstream, response_code,
                SmtpCommand::ResponseType::ViaUpstream);
  if (response_code == 334) {
    return result;
  } else if (response_code >= 400 && response_code <= 599) {
    callbacks_->incSmtpAuthErrors();
  } else if (response_code >= 200 && response_code <= 299) {
    setAuthStatus(true);
  }
  setState(SmtpSession::State::SessionInProgress);
  return result;
}

SmtpUtils::Result SmtpSession::handleStarttlsResponse(int& response_code, std::string& response) {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;

  if (getState() == SmtpSession::State::UpstreamTlsNegotiation) {
    if (response_code == 220) {
      if (callbacks_->upstreamStartTls()) {
        // Upstream TLS connection established.Now encrypt downstream connection.
        upstream_session_type_ = SmtpUtils::SessionType::Tls;
        handleDownstreamTls();
        result = SmtpUtils::Result::Stopped;
        return result;
      }
    }
    // We terminate this session if upstream server does not support TLS i.e. response code != 220
    // or if TLS handshake error occured with upstream
    storeResponse(response, SmtpUtils::via_upstream, response_code,
                  SmtpCommand::ResponseType::ViaUpstream);
    terminateSession(SmtpUtils::statusFailed, "Upstream TLS Handshake error");
    callbacks_->sendReplyDownstream(
        SmtpUtils::generateResponse(502, {5, 5, 1}, "TLS not supported"));
    callbacks_->closeDownstreamConnection();
    result = SmtpUtils::Result::Stopped;
    return result;
  }
  storeResponse(response, SmtpUtils::via_upstream, response_code,
                SmtpCommand::ResponseType::ViaUpstream);
  return result;
}

SmtpUtils::Result SmtpSession::handleOtherResponse(int& response_code, std::string& response) {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;

  storeResponse(response, SmtpUtils::via_upstream, response_code,
                SmtpCommand::ResponseType::ViaUpstream);
  return result;
}

SmtpUtils::Result SmtpSession::storeResponse(std::string response, std::string resp_code_details,
                                             int response_code,
                                             SmtpCommand::ResponseType resp_type) {

  if (current_command_ == nullptr)
    return SmtpUtils::Result::ReadyForNext;

  current_command_->onComplete(response, resp_code_details, response_code, resp_type);

  if (response_code >= 400 && response_code < 500) {
    if (current_command_->isLocalResponseSet()) {
      callbacks_->getStats().smtp_local_4xx_errors_.inc();
    } else {
      callbacks_->getStats().smtp_4xx_errors_.inc();
    }
  } else if (response_code >= 500 && response_code <= 599) {
    if (current_command_->isLocalResponseSet()) {
      callbacks_->getStats().smtp_local_5xx_errors_.inc();
    } else {
      callbacks_->getStats().smtp_5xx_errors_.inc();
    }
  }

  if (response_code / 100 == 3) {
    if (current_command_->getName() == SmtpUtils::smtpDataCommand) {
      command_length_->complete();
    }
    // If intermediate response code, do not end current command processing
    return SmtpUtils::Result::ReadyForNext;
  }

  if (current_command_->getName() != SmtpUtils::smtpDataCommand) {
    command_length_->complete();
  }
  command_in_progress_ = false;
  session_stats_.total_commands++;
  switch (current_command_->getType()) {
  case SmtpCommand::Type::NonTransactionCommand: {
    session_commands_.push_back(current_command_);
    break;
  }
  case SmtpCommand::Type::TransactionCommand: {
    getTransaction()->addTrxnCommand(current_command_);
    break;
  }
  default:
    break;
  };
  return SmtpUtils::Result::ReadyForNext;
}

void SmtpSession::encode(ProtobufWkt::Struct& metadata) {

  auto& fields = *(metadata.mutable_fields());

  // TODO: store total number of transaction and commands

  ProtobufWkt::Value total_transactions;
  total_transactions.set_number_value(session_stats_.total_transactions);
  fields["total_transactions"] = total_transactions;

  ProtobufWkt::Value transactions_completed;
  transactions_completed.set_number_value(session_stats_.transactions_completed);
  fields["transactions_completed"] = transactions_completed;

  ProtobufWkt::Value transactions_failed;
  transactions_failed.set_number_value(session_stats_.transactions_failed);
  fields["transactions_failed"] = transactions_failed;

  ProtobufWkt::Value transactions_aborted;
  transactions_aborted.set_number_value(session_stats_.transactions_aborted);
  fields["transactions_aborted"] = transactions_aborted;

  ProtobufWkt::Value total_commands;
  total_commands.set_number_value(session_stats_.total_commands);
  fields["total_commands"] = total_commands;

  ProtobufWkt::Value upstream_session_type;
  if (upstream_session_type_ == SmtpUtils::SessionType::PlainText) {
    upstream_session_type.set_string_value("PlainText");
  } else {
    upstream_session_type.set_string_value("TLS");
  }
  fields["upstream_session_type"] = upstream_session_type;

  ProtobufWkt::ListValue commands;
  for (auto command : session_commands_) {
    ProtobufWkt::Struct data_struct;
    auto& fields = *(data_struct.mutable_fields());

    ProtobufWkt::Value name;
    name.set_string_value(command->getName());
    fields["command_verb"] = name;

    ProtobufWkt::Value response_code;
    response_code.set_number_value(command->getResponseCode());
    fields["response_code"] = response_code;

    ProtobufWkt::Value response_code_details;
    response_code_details.set_string_value(command->getResponseCodeDetails());
    fields["response_code_details"] = response_code_details;

    ProtobufWkt::Value response_msg;
    response_msg.set_string_value(command->getResponseMsg());
    fields["msg"] = response_msg;

    ProtobufWkt::Value duration;
    duration.set_number_value(command->getDuration());
    fields["duration"] = duration;

    commands.add_values()->mutable_struct_value()->CopyFrom(data_struct);
  }
  fields["commands"].mutable_list_value()->CopyFrom(commands);
}

void SmtpSession::setSessionMetadata() {
  StreamInfo::StreamInfo& parent_stream_info = callbacks_->getStreamInfo();
  ProtobufWkt::Struct metadata(
      (*parent_stream_info.dynamicMetadata()
            .mutable_filter_metadata())[NetworkFilterNames::get().SmtpProxy]);

  auto& fields = *metadata.mutable_fields();
  // Emit SMTP session metadata
  ProtobufWkt::Struct session_metadata;
  encode(session_metadata);
  fields["session_metadata"].mutable_struct_value()->CopyFrom(session_metadata);

  ProtobufWkt::Value session_id;
  session_id.set_string_value(session_id_);
  fields["session_id"] = session_id;

  ProtobufWkt::Value log_type;
  log_type.set_string_value("session");
  fields["type"] = log_type;

  ProtobufWkt::Value status;
  status.set_string_value(status_);
  fields["status"] = status;

  ProtobufWkt::Value msg;
  msg.set_string_value(msg_);
  fields["msg"] = msg;

  parent_stream_info.setDynamicMetadata(NetworkFilterNames::get().SmtpProxy, metadata);
}

void SmtpSession::terminateSession(std::string status, std::string msg) {
  status_ = status;
  msg_ = msg;
  callbacks_->incSmtpSessionsTerminated();
  callbacks_->decActiveSession();
  setState(SmtpSession::State::SessionTerminated);
  if (transaction_in_progress_) {
    abortTransaction(SmtpUtils::trxnAbortedDueToSessionClose);
  }
  setSessionMetadata();
  session_length_->complete();

}

void SmtpSession::onSessionComplete() {
  status_ = SmtpUtils::statusSuccess;
  callbacks_->incSmtpSessionsCompleted();
  callbacks_->decActiveSession();
  setState(SmtpSession::State::SessionTerminated);
  if (transaction_in_progress_) {
    abortTransaction(SmtpUtils::trxnAbortedDueToSessionClose);
  }
  setSessionMetadata();
  session_length_->complete();
}

// void SmtpSession::endSession() {
//   callbacks_->decActiveSession();
//   setState(SmtpSession::State::SessionTerminated);

//   if (transaction_in_progress_) {
//     abortTransaction();
//   }
//   setSessionMetadata();
//   session_length_->complete();
// }

void SmtpSession::abortTransaction(std::string reason) {
  callbacks_->incSmtpTransactionsAborted();
  getTransaction()->setMsg(reason);
  getTransaction()->setStatus(SmtpUtils::statusAborted);
  getSessionStats().transactions_aborted++;
  endTransaction();
}

void SmtpSession::onTransactionComplete() {
  callbacks_->incSmtpTransactionsCompleted();
  getSessionStats().transactions_completed++;
  endTransaction();
}

void SmtpSession::onTransactionFailed(std::string& response) {
  getTransaction()->setStatus(SmtpUtils::statusFailed);
  getTransaction()->setMsg(response);
  getSessionStats().transactions_failed++;
  callbacks_->incSmtpTrxnFailed();
  endTransaction();
}

void SmtpSession::endTransaction() {

  // Emit access log for this transaction using transaction stream_info.
  session_stats_.total_transactions++;
  transaction_in_progress_ = false;
  callbacks_->decActiveTransaction();
  getTransaction()->onComplete();
  getTransaction()->emitLog();

  if (smtp_transaction_ != nullptr) {
    delete smtp_transaction_;
    smtp_transaction_ = nullptr;
  }
}

void SmtpSession::handleDownstreamTls() {
  setState(SmtpSession::State::DownstreamTlsNegotiation);
  getCurrentCommand()->storeLocalResponse("Ready to start TLS", "local", 220);
  if (!callbacks_->downstreamStartTls(
          SmtpUtils::generateResponse(220, {2, 0, 0}, "Ready to start TLS"))) {
    // callback returns false if connection is switched to tls i.e. tls termination is
    // successful.
    callbacks_->incTlsTerminatedSessions();
    setSessionEncrypted(true);
    setState(SmtpSession::State::SessionInProgress);
  } else {
    // error while switching transport socket to tls.
    callbacks_->incTlsTerminationErrors();
    getCurrentCommand()->storeLocalResponse("TLS Handshake error", "local", 550);
    callbacks_->sendReplyDownstream(
        SmtpUtils::generateResponse(550, {5, 0, 0}, "TLS Handshake error"));
    terminateSession(SmtpUtils::statusFailed, "downstream TLS Handshake error");
    callbacks_->closeDownstreamConnection();
  }
}

} // namespace SmtpProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

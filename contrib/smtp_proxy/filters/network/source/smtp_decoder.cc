
#include "contrib/smtp_proxy/filters/network/source/smtp_decoder.h"
#include "contrib/smtp_proxy/filters/network/source/smtp_utils.h"
#include "source/extensions/filters/network/well_known_names.h"
#include "source/common/common/logger.h"
#include "absl/strings/match.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace SmtpProxy {

Decoder::Result DecoderImpl::onData(Buffer::Instance& data, bool upstream) {
  Decoder::Result result = Decoder::Result::ReadyForNext;

  if (upstream) {
    result = parseResponse(data);
    data.drain(data.length());
    return result;
  }
  result = parseCommand(data);
  data.drain(data.length());
  return result;
}

Decoder::Result DecoderImpl::parseCommand(Buffer::Instance& data) {
  ENVOY_LOG(debug, "smtp_proxy: decoding {} bytes", data.length());
  Decoder::Result result = Decoder::Result::ReadyForNext;
  std::string command = data.toString();
  ENVOY_LOG(debug, "received command {}", command);
  if (command.length() < 6) {
    // Message size is not sufficient to parse.
    return result;
  }

  switch (session_.getState()) {
  case SmtpSession::State::CONNECTION_SUCCESS: {
    if (absl::StartsWithIgnoreCase(command, SmtpUtils::smtpEhloCommand) ||
        absl::StartsWithIgnoreCase(command, SmtpUtils::smtpHeloCommand)) {
      session_.setState(SmtpSession::State::SESSION_INIT_REQUEST);
    }
    break;
  }
  case SmtpSession::State::UPSTREAM_TLS_NEGOTIATION: {
    // No downstream messages will be processed when upstream TLS negotiation is in progress.
    callbacks_->sendReplyDownstream(SmtpUtils::mailboxUnavailableResponse);
    result = Decoder::Result::Stopped;
    break;
  }
  case SmtpSession::State::SESSION_IN_PROGRESS: {

    if (absl::StartsWithIgnoreCase(command, SmtpUtils::startTlsCommand)) {
      // STARTTLS command processing
      if (command.length() > 10) {
        ENVOY_LOG(error, "smtp_proxy: parameters not allowed for startls command");
        callbacks_->sendReplyDownstream(SmtpUtils::syntaxErrorNoParamsAllowed);
        result = Decoder::Result::Stopped;
        break;
      }
      if (session_.isSessionEncrypted()) {
        ENVOY_LOG(error, "smtp_proxy: received starttls when session is already encrypted.");
        callbacks_->sendReplyDownstream(SmtpUtils::outOfOrderCommandResponse);
        result = Decoder::Result::Stopped;
        break;
      }

      if (callbacks_->upstreamTlsRequired()) {
        // Send STARTTLS request to upstream.
        session_.setState(SmtpSession::State::UPSTREAM_TLS_NEGOTIATION);
        result = Decoder::Result::ReadyForNext;
        break;

      } else {
        // Perform downstream TLS negotiation.
        handleDownstreamTls();
        result = Decoder::Result::Stopped;
      }
      break;

    } else if (absl::StartsWithIgnoreCase(command, SmtpUtils::smtpQuitCommand)) {
      session_.setState(SmtpSession::State::SESSION_TERMINATION_REQUEST);
      break;
    } else if (absl::StartsWithIgnoreCase(command, SmtpUtils::smtpEhloCommand) ||
               absl::StartsWithIgnoreCase(command, SmtpUtils::smtpHeloCommand)) {
      session_.setState(SmtpSession::State::SESSION_INIT_REQUEST);
      break;
    } else if (absl::StartsWithIgnoreCase(command, SmtpUtils::smtpAuthCommand)) {
      session_.setState(SmtpSession::State::SESSION_AUTH_REQUEST);
      break;
    }

    decodeSmtpTransactionCommands(command);

    break;
  } // End case SESSION_IN_PROGRESS

  case SmtpSession::State::SESSION_TERMINATED: {
    result = Decoder::Result::Stopped;
    break;
  }
  default:
    break;
  }
  return result;
}

Decoder::Result DecoderImpl::parseResponse(Buffer::Instance& data) {
  ENVOY_LOG(debug, "smtp_proxy: decoding response {} bytes", data.length());

  Decoder::Result result = Decoder::Result::ReadyForNext;

  if (data.length() < 3) {
    // Minimum 3 byte response code needed to parse response from server.
    return result;
  }
  std::string response;
  response.assign(std::string(static_cast<char*>(data.linearize(3)), 3));

  uint16_t response_code = stoi(response);

  switch (session_.getState()) {

  case SmtpSession::State::CONNECTION_REQUEST: {
    if (response_code == 220) {
      session_.setState(SmtpSession::State::CONNECTION_SUCCESS);
    } else if (response_code == 554) {
      callbacks_->incSmtpConnectionEstablishmentErrors();
    }
    break;
  }

  case SmtpSession::State::SESSION_INIT_REQUEST: {
    if (response_code == 250) {
      session_.setState(SmtpSession::State::SESSION_IN_PROGRESS);
      if (session_.getTransactionState() == SmtpTransaction::State::TRANSACTION_IN_PROGRESS ||
          session_.getTransactionState() == SmtpTransaction::State::MAIL_DATA_TRANSFER_REQUEST) {
        // Increment stats for icomplete transactions when session is abruptly terminated.
        callbacks_->incSmtpTransactionsAborted();
        session_.SetTransactionState(SmtpTransaction::State::NONE);
      }
    }
    break;
  }

  case SmtpSession::State::SESSION_AUTH_REQUEST: {
    if (response_code == 334)
      break;
    if (response_code >= 400 && response_code <= 599) {
      callbacks_->incSmtpAuthErrors();
    }
    session_.setState(SmtpSession::State::SESSION_IN_PROGRESS);
    break;
  }

  case SmtpSession::State::DOWNSTREAM_TLS_NEGOTIATION: {
    break;
  }

  case SmtpSession::State::UPSTREAM_TLS_NEGOTIATION: {
    if (response_code == 220) {
      if (callbacks_->upstreamStartTls()) {
        // Upstream TLS connection established.Now encrypt downstream connection.
        handleDownstreamTls();
        result = Decoder::Result::Stopped;
        break;
      }
    }
    // If upstream server does not support TLS i.e. response code != 220
    // callbacks_->incUpstreamTlsFailed();
    session_.setState(SmtpSession::State::SESSION_TERMINATED);
    callbacks_->sendReplyDownstream(SmtpUtils::tlsNotSupportedResponse);
    callbacks_->closeDownstreamConnection();
    result = Decoder::Result::Stopped;
    break;
  }
  case SmtpSession::State::SESSION_IN_PROGRESS: {
    decodeSmtpTransactionResponse(response_code);
    break;
  }
  case SmtpSession::State::SESSION_TERMINATION_REQUEST: {
    if (response_code == 221) {
      session_.setState(SmtpSession::State::SESSION_TERMINATED);
      callbacks_->incSmtpSessionsCompleted();
      if (session_.getTransaction() && (session_.getTransactionState() == SmtpTransaction::State::TRANSACTION_IN_PROGRESS ||
          session_.getTransactionState() == SmtpTransaction::State::MAIL_DATA_TRANSFER_REQUEST)) {
        // Increment stats for incomplete transactions when session is abruptly terminated.
        callbacks_->incSmtpTransactionsAborted();
        session_.getTransaction()->setStatus(SmtpUtils::statusAborted);
        session_.resetTransaction();
      }
      setDynamicMetadata();
    } else {
      session_.setState(SmtpSession::State::SESSION_IN_PROGRESS);
    }
    break;
  }
  default:
    result = Decoder::Result::ReadyForNext;
  }
  return result;
}


void DecoderImpl::decodeSmtpTransactionCommands(std::string& command) {
  switch (session_.getTransactionState()) {
  case SmtpTransaction::State::NONE:
  case SmtpTransaction::State::TRANSACTION_COMPLETED: {

    if (absl::StartsWithIgnoreCase(command, SmtpUtils::smtpMailCommand)) {

      // session_.createNewTransaction();
      session_.SetTransactionState(SmtpTransaction::State::TRANSACTION_REQUEST);
    }
    break;
  }
  case SmtpTransaction::State::RCPT_COMMAND:
  case SmtpTransaction::State::TRANSACTION_IN_PROGRESS: {

    if(absl::StartsWithIgnoreCase(command, SmtpUtils::smtpRcptCommand)) {
      session_.SetTransactionState(SmtpTransaction::State::RCPT_COMMAND);
    } else if (absl::StartsWithIgnoreCase(command, SmtpUtils::smtpDataCommand)) {
      session_.SetTransactionState(SmtpTransaction::State::MAIL_DATA_TRANSFER_REQUEST);
    } else if (absl::StartsWithIgnoreCase(command, SmtpUtils::smtpRsetCommand) ||
               absl::StartsWithIgnoreCase(command, SmtpUtils::smtpEhloCommand) ||
               absl::StartsWithIgnoreCase(command, SmtpUtils::smtpHeloCommand)) {
      session_.SetTransactionState(SmtpTransaction::State::TRANSACTION_ABORT_REQUEST);
    }
    break;
  }
  case SmtpTransaction::State::MAIL_DATA_TRANSFER_REQUEST: {
    if (absl::StartsWithIgnoreCase(command, SmtpUtils::smtpRsetCommand) ||
               absl::StartsWithIgnoreCase(command, SmtpUtils::smtpEhloCommand) ||
               absl::StartsWithIgnoreCase(command, SmtpUtils::smtpHeloCommand)) {
      session_.SetTransactionState(SmtpTransaction::State::TRANSACTION_ABORT_REQUEST);
    }
  }
  default:
    break;
  }
}

void DecoderImpl::decodeSmtpTransactionResponse(uint16_t& response_code) {
  switch (session_.getTransactionState()) {
  case SmtpTransaction::State::TRANSACTION_REQUEST: {
    if (response_code == 250) {
      session_.SetTransactionState(SmtpTransaction::State::TRANSACTION_IN_PROGRESS);
    } else {
      session_.SetTransactionState(SmtpTransaction::State::NONE);
    }
    break;
  }
  case SmtpTransaction::State::RCPT_COMMAND: {
    if (response_code == 250 || response_code == 251) {
      session_.SetTransactionState(SmtpTransaction::State::TRANSACTION_IN_PROGRESS);
    } else if(response_code >=400 && response_code <= 599) {
      callbacks_->incMailRcptErrors();
    }
    break;
  }
  case SmtpTransaction::State::MAIL_DATA_TRANSFER_REQUEST: {
    session_.getTransaction()->setResponseCode(response_code);

    if (response_code == 250) {
      session_.SetTransactionState(SmtpTransaction::State::TRANSACTION_COMPLETED);
      session_.getTransaction()->setStatus(SmtpUtils::statusSuccess);
      callbacks_->incSmtpTransactions();
      session_.getSessionStats().transactions_completed++;
    } else if(response_code == 354) {
      //Intermediate response.
      break;
    } else if(response_code >=400 && response_code <= 599) {
      callbacks_->incMailDataTransferErrors();
      callbacks_->incSmtpTransactions();
      //Reset the transaction state in case of mail data transfer errors.
      session_.SetTransactionState(SmtpTransaction::State::NONE);
      session_.getTransaction()->setStatus(SmtpUtils::statusFailed);
      session_.getSessionStats().transactions_failed++;
    }
    session_.resetTransaction();
    break;
  }
  case SmtpTransaction::State::TRANSACTION_ABORT_REQUEST: {
    if (response_code == 250) {
      callbacks_->incSmtpTransactionsAborted();
      session_.SetTransactionState(SmtpTransaction::State::NONE);
      session_.getTransaction()->setStatus(SmtpUtils::statusAborted);
      session_.getSessionStats().transactions_aborted++;
      session_.resetTransaction();
    } else {
       session_.SetTransactionState(SmtpTransaction::State::TRANSACTION_IN_PROGRESS);
    }
    break;
  }
  default:
    break;
  }


}


void DecoderImpl::handleDownstreamTls() {
  session_.setState(SmtpSession::State::DOWNSTREAM_TLS_NEGOTIATION);
  if (!callbacks_->downstreamStartTls(SmtpUtils::readyToStartTlsResponse)) {
    // callback returns false if connection is switched to tls i.e. tls termination is
    // successful.
    session_.setSessionEncrypted(true);
    session_.setState(SmtpSession::State::SESSION_IN_PROGRESS);
  } else {
    // error while switching transport socket to tls.
    callbacks_->incTlsTerminationErrors();
    session_.setState(SmtpSession::State::SESSION_TERMINATED);
    callbacks_->sendReplyDownstream(SmtpUtils::tlsHandshakeErrorResponse);
    callbacks_->closeDownstreamConnection();
  }
}

void DecoderImpl::setDynamicMetadata() {

  // if(callbacks_->connection().ssl() != nullptr) {
  //   std::cout << callbacks_->connection().ssl()->tlsVersion() << std::endl;
  //   std::cout << callbacks_->connection().ssl()->ciphersuiteString() << std::endl;
  // } else {
  //   std::cout << "ssl info not available\n";
  // }

  ProtobufWkt::Struct metadata(
      (*stream_info_.dynamicMetadata().mutable_filter_metadata())[NetworkFilterNames::get().SmtpProxy]);

  auto& fields = *metadata.mutable_fields();

  //Emit SMTP session metadata
  ProtobufWkt::Struct session_metadata;
  session_.encode(session_metadata);
  fields["session_metadata"].mutable_struct_value()->CopyFrom(session_metadata);


  ProtobufWkt::ListValue transaction_metadata;
  session_.encodeTransactionMetadata(transaction_metadata);
  fields["smtp_transactions"].mutable_list_value()->CopyFrom(transaction_metadata);

  // auto& transactions = *fields["smtp_transactions"].mutable_list_value();
  // transactions.add_values()->mutable_struct_value()->CopyFrom(transaction_metadata);

  stream_info_.setDynamicMetadata(
      NetworkFilterNames::get().SmtpProxy, metadata);
}

} // namespace SmtpProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
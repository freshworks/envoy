#pragma once
#include <cstdint>
#include <string>

#include "source/common/common/logger.h"
#include "source/common/protobuf/utility.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace SmtpProxy {

// Class stores data about the current state of a transaction between SMTP client and server.

class SmtpTransaction {
public:
  enum class State {
    NONE = 0,
    TRANSACTION_REQUEST = 1,
    TRANSACTION_IN_PROGRESS = 2,
    TRANSACTION_ABORT_REQUEST = 3,
    TRANSACTION_ABORTED = 4,
    MAIL_DATA_TRANSFER_REQUEST = 5,
    RCPT_COMMAND = 6,
    TRANSACTION_COMPLETED = 7,
  };

  SmtpTransaction(int id) : transaction_id_(id), status_("NONE") {}

  int getTransactionId() { return transaction_id_; }
  void setState(SmtpTransaction::State state) { state_ = state; }
  SmtpTransaction::State getState() { return state_; }

  void setResponseCode(uint16_t response_code) { response_code_ = response_code; }
  uint16_t getResponseCode() { return response_code_; }

  void setStatus(const std::string status) { status_ = status;}
  const std::string& getStatus() const { return status_; }

  void encode(ProtobufWkt::Struct& metadata);

private:
  SmtpTransaction::State state_{State::NONE};
  int transaction_id_{0};

  //The response code for last command in a transaction.
  uint16_t response_code_{0};
  //Transaction status
  std::string status_;
};


} // namespace SmtpProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

#pragma once
#include <cstdint>
#include "contrib/smtp_proxy/filters/network/source/smtp_transaction.h"
#include "source/common/common/logger.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace SmtpProxy {

class SmtpSession {
public:
  enum class State {
    CONNECTION_REQUEST = 0,
    CONNECTION_SUCCESS = 1,
    SESSION_INIT_REQUEST = 2,
    SESSION_IN_PROGRESS = 3,
    SESSION_TERMINATION_REQUEST = 4,
    SESSION_TERMINATED = 5,
    UPSTREAM_TLS_NEGOTIATION = 6,
    DOWNSTREAM_TLS_NEGOTIATION = 7,
    SESSION_AUTH_REQUEST = 8,
  };

  struct SmtpSessionStats {
    int transactions_failed;
    int transactions_completed;
    int transactions_aborted;
    int downstream_tls_success;
    int downstream_tls_failed;
    int upstream_tls_success;
    int upstream_tls_failed;
  };

  SmtpSession() {
    smtp_transaction_ =  new SmtpTransaction(++current_transaction_id);
  }

  ~SmtpSession() {
    delete smtp_transaction_;
    smtp_transaction_ = nullptr;
  }

  void setState(SmtpSession::State state) { state_ = state; }
  SmtpSession::State getState() { return state_; }

  SmtpTransaction* getTransaction(){ return smtp_transaction_; }
  void createNewTransaction();
  void deleteTransaction();

  void SetTransactionState(SmtpTransaction::State state) { smtp_transaction_->setState(state); };
  SmtpTransaction::State getTransactionState() { return smtp_transaction_->getState(); }

  int getCurrentTransactionId() { return current_transaction_id; }
  SmtpSession::SmtpSessionStats& getSessionStats() { return session_stats_; }

  void setSessionEncrypted(bool flag) { session_encrypted_ = flag; }
  bool isSessionEncrypted() const { return session_encrypted_; }

  void encode(ProtobufWkt::Struct& metadata);

private:
  SmtpSession::State state_{State::CONNECTION_REQUEST};
  SmtpTransaction* smtp_transaction_;
  SmtpSession::SmtpSessionStats session_stats_;
  bool session_encrypted_{false}; // tells if exchange is encrypted
  int current_transaction_id{0};

};

} // namespace SmtpProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

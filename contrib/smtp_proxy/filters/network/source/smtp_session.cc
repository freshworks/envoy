#include "contrib/smtp_proxy/filters/network/source/smtp_session.h"
#include "source/common/common/logger.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace SmtpProxy {

void SmtpSession::createNewTransaction() {
  if(smtp_transaction_ == nullptr)
  {
    smtp_transaction_ = new SmtpTransaction(++current_transaction_id);
  }
}

void SmtpSession::deleteTransaction() {
  if(smtp_transaction_ != nullptr) {
    delete smtp_transaction_;
    smtp_transaction_ = nullptr;
  }
}

void SmtpSession::encode(ProtobufWkt::Struct& metadata) {

  auto& fields = *(metadata.mutable_fields());

  ProtobufWkt::Value total_transactions;
  total_transactions.set_number_value(getCurrentTransactionId());
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

}

} // namespace SmtpProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

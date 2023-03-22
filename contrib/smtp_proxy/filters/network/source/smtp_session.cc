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

void SmtpSession::resetTransaction() {
  if(smtp_transaction_ != nullptr) {

    SmtpTransactionMetadata* trxn_metadata = new SmtpTransactionMetadata();
    trxn_metadata->transaction_id = smtp_transaction_->getTransactionId();
    trxn_metadata->response_code = smtp_transaction_->getResponseCode();
    trxn_metadata->status = smtp_transaction_->getStatus();

    transaction_metadata_.push_back(trxn_metadata);

    delete smtp_transaction_;
    smtp_transaction_ = nullptr;
  }

  smtp_transaction_ = new SmtpTransaction(++current_transaction_id);
}

void SmtpSession::encode(ProtobufWkt::Struct& metadata) {

  auto& fields = *(metadata.mutable_fields());

  ProtobufWkt::Value total_transactions;
  total_transactions.set_number_value(getCurrentTransactionId() - 1);
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

void SmtpSession::encodeTransactionMetadata(ProtobufWkt::ListValue& transaction_metadata_list) {

  for (auto& trxn_metdata : transaction_metadata_) {
      ProtobufWkt::Struct data_struct;
      auto& fields = *(data_struct.mutable_fields());

      ProtobufWkt::Value transaction_id;
      transaction_id.set_number_value(trxn_metdata->transaction_id);
      fields["transaction_id"] = transaction_id;

      ProtobufWkt::Value status;
      status.set_string_value(trxn_metdata->status);
      fields["status"] = status;

      ProtobufWkt::Value response_code;
      response_code.set_number_value(trxn_metdata->response_code);
      fields["response_code"] = response_code;

      transaction_metadata_list.add_values()->mutable_struct_value()->CopyFrom(data_struct);
    }
}

} // namespace SmtpProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

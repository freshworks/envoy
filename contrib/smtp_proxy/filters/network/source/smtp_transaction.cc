#include "contrib/smtp_proxy/filters/network/source/smtp_transaction.h"
#include "source/common/common/logger.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace SmtpProxy {


// void SmtpTransaction::setTransactionMetadata(TransactionMetadata* trxn_metdata_obj) {

//   trxn_metdata_obj->transaction_id = transaction_id_;
//   trxn_metdata_obj->response_code = response_code_;
//   trxn_metdata_obj->status = getStatus();
// }
void SmtpTransaction::encode(ProtobufWkt::Struct& metadata) {

  auto& fields = *(metadata.mutable_fields());

  ProtobufWkt::Value transaction_id;
  transaction_id.set_number_value(transaction_id_);
  fields["transaction_id"] = transaction_id;

  ProtobufWkt::Value status;
  status.set_string_value(getStatus());
  fields["status"] = status;

  ProtobufWkt::Value response_code;
  response_code.set_number_value(response_code_);
  fields["response_code"] = response_code;

}


} // namespace SmtpProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

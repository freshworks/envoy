#pragma once
#include "envoy/stats/scope.h"
#include "envoy/stats/stats.h"
#include "envoy/stats/stats_macros.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace SmtpProxy {

/**
 * All SMTP proxy stats. @see stats_macros.h
 */
#define ALL_SMTP_PROXY_STATS(COUNTER, GUAGE, HISTOGRAM)                                            \
  COUNTER(smtp_session_requests)                                                                   \
  COUNTER(smtp_connection_establishment_errors)                                                    \
  COUNTER(smtp_session_completed)                                                                  \
  COUNTER(smtp_session_terminated)                                                                 \
  COUNTER(smtp_transaction_req)                                                                    \
  COUNTER(smtp_transaction_completed)                                                              \
  COUNTER(smtp_transaction_failed)                                                                 \
  COUNTER(smtp_transaction_aborted)                                                                \
  COUNTER(smtp_session_tls_termination_success)                                                    \
  COUNTER(smtp_session_tls_termination_error)                                                      \
  COUNTER(smtp_session_upstream_tls_success)                                                       \
  COUNTER(smtp_session_upstream_tls_failed)                                                        \
  COUNTER(smtp_auth_errors)                                                                        \
  COUNTER(smtp_mail_data_transfer_errors)                                                          \
  COUNTER(smtp_rcpt_errors)                                                                        \
  COUNTER(smtp_4xx_errors)                                                                         \
  COUNTER(smtp_5xx_errors)                                                                         \
  GUAGE(smtp_transaction_active, Accumulate)                                                       \
  GUAGE(smtp_session_active, Accumulate)                                                           \
  HISTOGRAM(smtp_transaction_length, Milliseconds)                                                 \
  HISTOGRAM(smtp_session_length, Milliseconds)                                                     \
  HISTOGRAM(smtp_data_transfer_length, Milliseconds)                                               \
  HISTOGRAM(smtp_command_length, Milliseconds)

/**
 * Struct definition for all SMTP proxy stats. @see stats_macros.h
 */
struct SmtpProxyStats {
  ALL_SMTP_PROXY_STATS(GENERATE_COUNTER_STRUCT, GENERATE_GAUGE_STRUCT, GENERATE_HISTOGRAM_STRUCT)
};

} // namespace SmtpProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

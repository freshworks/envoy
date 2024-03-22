#pragma once

#include <string>

#include "source/extensions/filters/network/common/redis/codec.h"
#include "source/common/common/logger.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace Common {
namespace Redis {
namespace Utility {

struct DownStreamMetrics {
  uint64_t downstream_rq_total_{};
  uint64_t downstream_cx_drain_close_{};
  uint64_t downstream_cx_protocol_error_{};
  uint64_t downstream_cx_rx_bytes_total_{};
  uint64_t downstream_cx_total_{};
  uint64_t downstream_cx_tx_bytes_total_{};
  uint64_t downstream_cx_active_{};
  uint64_t downstream_cx_rx_bytes_buffered_{};
  uint64_t downstream_cx_tx_bytes_buffered_{};
  uint64_t downstream_rq_active_{};
  uint64_t connection_id_{};
  std::string local_address_{};
  std::string remote_address_{};
};


enum InforesponseAggregatorType {
  staticvalue,
  sumvalues,
  highestvalue,
  customizevalue,
  hardcodedvalue,
  proxymetrics
};


class InfoCmdResponseProcessor:public Logger::Loggable<Logger::Id::redis> {
public:

  InfoCmdResponseProcessor(DownStreamMetrics& downstream_metrics);
  ~InfoCmdResponseProcessor();

  void processInfoCmdResponse(const std::string& infokey, const std::string& infovalue);
  std::string getInfoCmdResponseString();

  struct infoCmdResponseDecoder {
    std::string infocategory;
    std::string infokey;
    enum InforesponseAggregatorType aggregate_type;
    std::string strvalue;
    std::uint64_t intvalue;
    void (*customizer)(const std::string& infokey, const std::string& infovalue,infoCmdResponseDecoder& infoObj);
    InfoCmdResponseProcessor* processor;
  };

  void updateInfoResponseWithProxyMetrics(const std::string& infokey,infoCmdResponseDecoder& infoObj);
  DownStreamMetrics& getDownstreamMetrics() { return downstream_metrics_; }
private:

  std::vector<infoCmdResponseDecoder> inforesponsetemplate_;
  std::unordered_map<std::string, size_t> converters_;
  DownStreamMetrics& downstream_metrics_;
};

class AuthRequest : public Redis::RespValue {
public:
  AuthRequest(const std::string& username, const std::string& password);
  AuthRequest(const std::string& password);
};

RespValuePtr makeError(const std::string& error);

class ReadOnlyRequest : public Redis::RespValue {
public:
  ReadOnlyRequest();
  static const ReadOnlyRequest& instance();
};

class AskingRequest : public Redis::RespValue {
public:
  AskingRequest();
  static const AskingRequest& instance();
};

class GetRequest : public Redis::RespValue {
public:
  GetRequest();
  static const GetRequest& instance();
};

class SetRequest : public Redis::RespValue {
public:
  SetRequest();
  static const SetRequest& instance();
};

} // namespace Utility
} // namespace Redis
} // namespace Common
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

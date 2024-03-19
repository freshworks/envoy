#include "source/extensions/filters/network/common/redis/utility.h"

#include "source/common/common/utility.h"
#include "absl/strings/str_split.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace Common {
namespace Redis {
namespace Utility {

AuthRequest::AuthRequest(const std::string& password) {
  std::vector<RespValue> values(2);
  values[0].type(RespType::BulkString);
  values[0].asString() = "auth";
  values[1].type(RespType::BulkString);
  values[1].asString() = password;
  type(RespType::Array);
  asArray().swap(values);
}

AuthRequest::AuthRequest(const std::string& username, const std::string& password) {
  std::vector<RespValue> values(3);
  values[0].type(RespType::BulkString);
  values[0].asString() = "auth";
  values[1].type(RespType::BulkString);
  values[1].asString() = username;
  values[2].type(RespType::BulkString);
  values[2].asString() = password;
  type(RespType::Array);
  asArray().swap(values);
}

RespValuePtr makeError(const std::string& error) {
  Common::Redis::RespValuePtr response(new RespValue());
  response->type(Common::Redis::RespType::Error);
  response->asString() = error;
  return response;
}

ReadOnlyRequest::ReadOnlyRequest() {
  std::vector<RespValue> values(1);
  values[0].type(RespType::BulkString);
  values[0].asString() = "readonly";
  type(RespType::Array);
  asArray().swap(values);
}

const ReadOnlyRequest& ReadOnlyRequest::instance() {
  static const ReadOnlyRequest* instance = new ReadOnlyRequest{};
  return *instance;
}

AskingRequest::AskingRequest() {
  std::vector<RespValue> values(1);
  values[0].type(RespType::BulkString);
  values[0].asString() = "asking";
  type(RespType::Array);
  asArray().swap(values);
}

const AskingRequest& AskingRequest::instance() {
  static const AskingRequest* instance = new AskingRequest{};
  return *instance;
}

GetRequest::GetRequest() {
  type(RespType::BulkString);
  asString() = "get";
}

const GetRequest& GetRequest::instance() {
  static const GetRequest* instance = new GetRequest{};
  return *instance;
}

SetRequest::SetRequest() {
  type(RespType::BulkString);
  asString() = "set";
}

const SetRequest& SetRequest::instance() {
  static const SetRequest* instance = new SetRequest{};
  return *instance;
}

void CommonInfoCmdResponseAggregator(const std::string& infokey,const std::string& infovalue,InfoCmdResponseProcessor::infoCmdResponseDecoder& infoCmdObj){

  // Handle unused variable warning
  (void)infokey;
  switch (infoCmdObj.aggregate_type) {
  case staticvalue:
    infoCmdObj.strvalue = infovalue;
    break;
  case sumvalues:
    infoCmdObj.intvalue += std::stoull(infovalue);
    infoCmdObj.strvalue = std::to_string(infoCmdObj.intvalue);
    break;
  case highestvalue:
    if (std::stoull(infovalue) > infoCmdObj.intvalue) {
      infoCmdObj.intvalue = std::stoull(infovalue);
      infoCmdObj.strvalue = infovalue;
    }
    break;
  case proxymetrics:
    infoCmdObj.processor->updateInfoResponseWithProxyMetrics(infokey,infoCmdObj);
    break;
  default:
    break;
  }

  return;
}

void getkeyspacestats(const std::string& value, std::uint64_t& keys, std::uint64_t& expires, std::uint64_t& avg_ttl) {
  std::vector<std::string> keyvaluepairs = absl::StrSplit(value, ',');

  for (const auto& keyvaluepair : keyvaluepairs) {
    std::vector<std::string> keyvalue = absl::StrSplit(keyvaluepair, '=');
    if (keyvalue.size() == 2) {
      if (keyvalue[0] == "keys") {
        keys = std::stoull(keyvalue[1]);
      } else if (keyvalue[0] == "expires") {
        expires = std::stoull(keyvalue[1]);
      } else if (keyvalue[0] == "avg_ttl") {
        avg_ttl = std::stoull(keyvalue[1]);
      }
    }
  }
}

void InfoResponseAggrKeyspace(const std::string& infokey, const std::string& infovalue,InfoCmdResponseProcessor::infoCmdResponseDecoder& infoCmdObj){

  std::uint64_t currkeys,oldkeys = 0;
  std::uint64_t currexpires,oldexpires = 0;
  std::uint64_t curravg_ttl,oldavg_ttl = 0;

  if (infoCmdObj.strvalue != ""){
    getkeyspacestats(infoCmdObj.strvalue, oldkeys, oldexpires, oldavg_ttl);
  }

  if (infokey == "db0") {
    getkeyspacestats(infovalue, currkeys, currexpires, curravg_ttl);
    infoCmdObj.strvalue = "keys=" + std::to_string(currkeys + oldkeys) + ",expires=" + std::to_string(currexpires + oldexpires) + ",avg_ttl=" + std::to_string((curravg_ttl + oldavg_ttl) / 2);
  }

  return;

}
void InfoCmdResponseProcessor::updateInfoResponseWithProxyMetrics(const std::string& infokey,InfoCmdResponseProcessor::infoCmdResponseDecoder& infoCmdObj){

if (infokey == "total_connections_received"){
  auto cxtotal = infoCmdObj.processor->getDownstreamMetrics().downstream_cx_total_;
  infoCmdObj.strvalue = std::to_string(cxtotal);
}
return;

}


std::string InfoCmdResponseProcessor::getInfoCmdResponseString(){

  //generate info response string from the inforesponsetemplate_ vector
  std::string prevInfoCategory="";
  std::string infoResponseString;
  for (const auto& entry : inforesponsetemplate_) {
    
    if (prevInfoCategory != entry.infocategory) {
      infoResponseString +=  entry.infocategory + "\n";
      prevInfoCategory = entry.infocategory;
    }
    if (entry.strvalue != ""){
      infoResponseString += entry.infokey + ":" + entry.strvalue + "\n";
    }
  }
  return infoResponseString;

}

InfoCmdResponseProcessor::InfoCmdResponseProcessor(DownStreamMetrics& downstream_metrics) : downstream_metrics_(downstream_metrics){

  //Initialize the info response template
  inforesponsetemplate_ = {
    {"# Server","redis_version", staticvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Server","redis_git_sha1", staticvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Server","redis_git_dirty", staticvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Server","redis_build_id", staticvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Server","redis_mode", hardcodedvalue, "standalone", 0, nullptr,nullptr},
    {"# Server","os", staticvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Server","arch_bits", staticvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Server","multiplexing_api", staticvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Server","process_id", staticvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Server","run_id", staticvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    //{"# Server","tcp_port", staticvalue, "", 0, CommonInfoCmdResponseAggregator}, //Need to find a way to get the TCP port from listener , currently its not exposed
    {"# Server","uptime_in_seconds", highestvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Server","uptime_in_days", highestvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Server","hz", staticvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Server","lru_clock", staticvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Server","config_file", staticvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Memory","used_memory", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Memory","used_memory_peak", highestvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Memory","used_memory_rss", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Memory","used_memory_lua", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Memory","maxmemory", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Memory","maxmemory_policy", staticvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Memory","mem_fragmentation_ratio", highestvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Memory","mem_allocator", staticvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Persistence","loading", highestvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Persistence","rdb_changes_since_last_save", highestvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Persistence","rdb_bgsave_in_progress", highestvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Persistence","rdb_last_save_time", highestvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Persistence","rdb_last_bgsave_status", staticvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Persistence","rdb_last_bgsave_time_sec", highestvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Persistence","rdb_current_bgsave_time_sec", highestvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","total_connections_received", proxymetrics, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","total_commands_processed", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","instantaneous_ops_per_sec", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","total_net_input_bytes", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","total_net_output_bytes", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","instantaneous_input_kbps", highestvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","instantaneous_output_kbps", highestvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","rejected_connections", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","sync_full", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","sync_partial_ok", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","sync_partial_err", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","expired_keys", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","evicted_keys", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","keyspace_hits", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","keyspace_misses", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","pubsub_channels", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","pubsub_patterns", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Stats","latest_fork_usec", highestvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Replication","role", staticvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Replication","connected_slaves", staticvalue, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# CPU","used_cpu_sys", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# CPU","used_cpu_user", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# CPU","used_cpu_sys_children", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# CPU","used_cpu_user_children", sumvalues, "", 0, CommonInfoCmdResponseAggregator,nullptr},
    {"# Cluster","cluster_enabled", hardcodedvalue, "0", 0, nullptr,nullptr},
    {"# Keyspace","db0",customizevalue, "", 0, InfoResponseAggrKeyspace,nullptr}
  };
  // Populate the converters_ hash map with indices of the inforesponsetemplate_ vector
    for (size_t i = 0; i < inforesponsetemplate_.size(); ++i) {
        converters_[inforesponsetemplate_[i].infokey] = i;
        inforesponsetemplate_[i].processor = this;
    }
}

InfoCmdResponseProcessor::~InfoCmdResponseProcessor() {
}

void InfoCmdResponseProcessor::processInfoCmdResponse(const std::string& key, const std::string& value) {
  auto it = converters_.find(key);
  if (it != converters_.end()) {
    // Use the found index to access the corresponding entry in inforesponsetemplate_
    infoCmdResponseDecoder& entry = inforesponsetemplate_[it->second];

    if (entry.customizer != nullptr) {
      entry.customizer(key, value, entry);
      //ENVOY_LOG(debug, "infokey: '{}' infovalue: '{}'", inforesponsetemplate_[it->second].infokey, inforesponsetemplate_[it->second].strvalue);
    }

  }
  return;
}

} // namespace Utility
} // namespace Redis
} // namespace Common
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

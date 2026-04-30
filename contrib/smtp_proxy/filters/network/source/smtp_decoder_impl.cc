#include "contrib/smtp_proxy/filters/network/source/smtp_decoder_impl.h"

#include "source/common/common/utility.h"

#include "absl/strings/match.h"
#include "absl/strings/str_split.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace SmtpProxy {

SmtpUtils::Result DecoderImpl::parseCommand(Buffer::Instance& data, Command& command) {
  ENVOY_LOG(debug, "smtp_proxy parseCommand: decoding {} bytes", data.length());
  ENVOY_LOG(debug, "smtp_proxy received command: {}", data.toString());

  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;

  std::string current_line;
  command.len = data.length();
  result = isValidSmtpLine(data, SmtpUtils::maxCommandLen, current_line);
  if (result != SmtpUtils::Result::ReadyForNext) {
    return result;
  }
  if (current_line.length() != data.length()) {
    return SmtpUtils::Result::ProtocolError;
  }
  // std::string commandStr = current_line.substr(0, crlfPos);
  absl::string_view commandStr = StringUtil::cropRight(current_line, SmtpUtils::CRLF);
  // Split the command into verb and arguments
  size_t spacePos = commandStr.find(' ');
  
  if (spacePos == 0) {
    return SmtpUtils::Result::ProtocolError;
  }
  command.verb = (spacePos != std::string::npos) ? commandStr.substr(0, spacePos) : commandStr;
  command.args = (spacePos != std::string::npos) ? commandStr.substr(spacePos + 1) : "";


  ENVOY_LOG(debug, "command verb {}", command.verb);
  ENVOY_LOG(debug, "command args {}", command.args);

  data.drain(data.length());
  return result;
}

SmtpUtils::Result DecoderImpl::isValidSmtpLine(Buffer::Instance& data, size_t max_len,
                                               std::string& output) {
  size_t crlfPos = data.search(SmtpUtils::CRLF.data(), SmtpUtils::CRLF.size(), 0, max_len);
  if (crlfPos == std::string::npos) {
    // Received data that does not contain /r/n, possibly received incomplete data.
    // But we also check if length of received data is more than allowed limit.
    if (data.length() >= max_len) {
      return SmtpUtils::Result::ProtocolError;
    }
    return SmtpUtils::Result::NeedMoreData;
  }

  if (crlfPos <= 0) {
    return SmtpUtils::Result::ProtocolError;
  }

  std::string buffer = data.toString();
  output = buffer.substr(0, crlfPos + SmtpUtils::CRLF.size());
  return SmtpUtils::Result::ReadyForNext;
}

SmtpUtils::Result DecoderImpl::parseResponse(Buffer::Instance& data, Response& response) {
  ENVOY_LOG(debug, "smtp_proxy: decoding response {} bytes", data.length());
  ENVOY_LOG(debug, "smtp_proxy: decoding response {}", data.toString());

  int response_code = 0;
  std::string response_msg;
  size_t respose_len = 0;
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;
  Buffer::OwnedImpl buffer(data);
  // Loop to parse multi-line response
  // https://www.rfc-editor.org/rfc/rfc5321.html#section-4.2
  //  Reply-line     = *( Reply-code "-" [ textstring ] CRLF )
  //                 Reply-code [ SP textstring ] CRLF
  //  Reply-code     = %x32-35 %x30-35 %x30-39
  while (true) {
    std::string current_line;
    result = isValidSmtpLine(buffer, SmtpUtils::maxResponseLen, current_line);
    if (result != SmtpUtils::Result::ReadyForNext) {
      return result;
    }
    buffer.drain(current_line.size());
    respose_len += current_line.length();
    // A response has to be of minimum 3 char length i.e response code needs to be present
    if (current_line.length() < 3) {
      return SmtpUtils::Result::ProtocolError;
    }

    std::string response_code_str = current_line.substr(0, 3);
    int code = 0;
    std::string msg;
    try {
      code = stoi(response_code_str);
    } catch (...) {
      code = 0;
      ENVOY_LOG(error, "smtp_proxy: error while decoding response code ", response_code);
      return SmtpUtils::Result::ProtocolError;
    }
    if (response_code && code != response_code) {
      return SmtpUtils::Result::ProtocolError;
    }
    response_code = code;
    current_line = current_line.erase(0, 3);
    // Separator can be either ' ' or '-'
    char separator = ' ';
    if (!current_line.empty() && current_line != SmtpUtils::CRLF.data()) {
      separator = current_line[0];
      if (separator != ' ' && separator != '-') {
        return SmtpUtils::Result::ProtocolError;
      }
      // Remove the first character from line
      current_line = current_line.erase(0, 1);
      response_msg += current_line;
    }

    if (separator == ' ') {
      break; // We reached last line of reply.
    }
  }

  response.len = respose_len;
  size_t crlf_pos = response_msg.length() - 2;
  response_msg = response_msg.erase(crlf_pos);
  // ENVOY_LOG(debug, "smtp_proxy: response code {}", response_code);
  // ENVOY_LOG(debug, "smtp_proxy: response msg {}",response_msg);
  response.resp_code = response_code;
  response.msg = response_msg;
  // response.len = respose_len;
  data.drain(data.length());
  return SmtpUtils::Result::ReadyForNext;
}

} // namespace SmtpProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

#include "contrib/smtp_proxy/filters/network/source/smtp_decoder_impl.h"

#include "source/common/common/logger.h"
#include "source/extensions/filters/network/well_known_names.h"

#include "absl/strings/match.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace SmtpProxy {

namespace {
  static constexpr absl::string_view CRLF = "\r\n";
  constexpr size_t kMaxCommandLine = 1024;
  constexpr size_t kMaxReplyLine = 1024;
  const std::string known_commands[] = {"HELO", "EHLO", "AUTH", "MAIL", "RCPT", "DATA", "QUIT", "RSET", "STARTTLS", "XREQID"};
}


DecoderImpl::DecoderImpl(DecoderCallbacks* callbacks, SmtpSession* session, TimeSource& time_source,
                         Random::RandomGenerator& random_generator)
    : callbacks_(callbacks), session_(session), time_source_(time_source), random_generator_(random_generator) {
  // session_ = new SmtpSession(callbacks, time_source_, random_generator_);
}

SmtpUtils::Result DecoderImpl::onData(Buffer::Instance& data, bool upstream) {
  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;
  if (upstream) {
    result = parseResponse(data);
    // data.drain(data.length());
    return result;
  }
  result = parseCommand(data);
  // data.drain(data.length());
  return result;
}

SmtpUtils::Result DecoderImpl::parseCommand(Buffer::Instance& data) {
  ENVOY_LOG(debug, "smtp_proxy parseCommand: decoding {} bytes", data.length());
  ENVOY_LOG(debug, "smtp_proxy received command: {}", data.toString());

  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;

  if (getSession()->isTerminated()) {
    data.drain(data.length());
    return result;
  }
  if (getSession()->isDataTransferInProgress()) {
    getSession()->updateBytesMeterOnCommand(data);
    data.drain(data.length());
    return result;
  }

  // std::string buffer = data.toString();
  // ENVOY_LOG(debug, "received command {}", buffer);
  // // Each SMTP command ends with CRLF ("\r\n"), if received data doesn't end with CRLF, the decoder
  // // will buffer the data until it receives CRLF .
  // // command should end with /r/n
  // //  There shouldn't be multiple /r/n
  // //  Extract the command verb and args, only if verb is known, process it. else pass it to upstream.
  // if (!absl::EndsWith(buffer, SmtpUtils::smtpCrlfSuffix)) {
  //   result = SmtpUtils::Result::NeedMoreData;
  //   return result;
  // }

  // buffer = StringUtil::cropRight(buffer, SmtpUtils::smtpCrlfSuffix);

  // const bool trailing_crlf_consumed = absl::ConsumeSuffix(&buffer, CRLF);
  // ASSERT(trailing_crlf_consumed);

  // There shouldn't be second CRLF present in received command. If so, do not process this command.
  // const ssize_t crlf = buffer.search(CRLF, CRLF.size(), 0, max_line);
  // int length = buffer.length();


  std::string line;
  result = getLine(data, kMaxCommandLine, line);
  if (result == SmtpUtils::Result::ProtocolError) {
    data.drain(data.length());
    return result;
  } else if (result == SmtpUtils::Result::NeedMoreData) {
    return result;
  }
  absl::string_view line_remaining(line);

  // Case when mulitple \r\n are found.
  if (line_remaining.size() != data.length()) {
    data.drain(data.length());
    return SmtpUtils::Result::ProtocolError;
  }

  const bool trailing_crlf_consumed = absl::ConsumeSuffix(&line_remaining, CRLF);
  ASSERT(trailing_crlf_consumed);

  const size_t space = line_remaining.find(' ');
  std::string verb;
  std::string args;
  if (space == absl::string_view::npos) {
    // no arguments e.g.
    // NOOP\r\n
    verb = std::string(line_remaining);
  } else {
    // EHLO mail.example.com\r\n
    verb = std::string(line_remaining.substr(0, space));
    args = std::string(line_remaining.substr(space + 1));
  }

  ENVOY_LOG(debug, "command verb {}", verb);
  ENVOY_LOG(debug, "command args {}", args);

  for (const auto& c : known_commands) {
    if (absl::AsciiStrToUpper(verb) != c) {
      continue;
    }
    result = session_->handleCommand(verb, args);
    break;
  }

  data.drain(data.length());
  return result;
}

SmtpUtils::Result DecoderImpl::getLine(Buffer::Instance& data, size_t max_line, std::string& line_out) {
  const ssize_t crlf = data.search(CRLF.data(), CRLF.size(), 0, max_line);
  if (crlf == -1) {
    if (data.length() >= max_line) {
      return SmtpUtils::Result::ProtocolError;
    } else {
      return SmtpUtils::Result::NeedMoreData;
    }
  }
  ASSERT(crlf >= 0);
  // empty line is invalid
  if (crlf == 0) {
    return SmtpUtils::Result::ProtocolError;
  }

  const size_t len = crlf + CRLF.size();
  // std::cout << "len = " << len << std::endl;
  ASSERT(len > CRLF.size());
  auto front_slice = data.frontSlice();
  // std::cout << "front_slice.len = " << front_slice.len_ << std::endl;
  if (front_slice.len_ >= len) {
    line_out.append(static_cast<char*>(front_slice.mem_), len);
  } else {
    std::unique_ptr<char[]> tmp(new char[len]);
    data.copyOut(0, len, tmp.get());
    line_out.append(tmp.get(), len);
  }
  return SmtpUtils::Result::ReadyForNext;
}

SmtpUtils::Result DecoderImpl::parseResponse(Buffer::Instance& data) {
  ENVOY_LOG(debug, "smtp_proxy: decoding response {} bytes", data.length());
  ENVOY_LOG(debug, "smtp_proxy: received response {}", data.toString());

  SmtpUtils::Result result = SmtpUtils::Result::ReadyForNext;

  std::string line;
  result = getLine(data, kMaxReplyLine, line);
  if (result == SmtpUtils::Result::ProtocolError) {
    data.drain(data.length());
    return result;
  } else if (result == SmtpUtils::Result::NeedMoreData) {
    return result;
  }

  // https://www.rfc-editor.org/rfc/rfc5321.html#section-4.2
  //  Reply-line     = *( Reply-code "-" [ textstring ] CRLF )
  //                 Reply-code [ SP textstring ] CRLF
  //  Reply-code     = %x32-35 %x30-35 %x30-39
  if (line.length() < 3) {
    data.drain(data.length());
    return SmtpUtils::Result::ProtocolError;
  }

  std::string response_code_str = line.substr(0, 3);
  int response_code = 0;
  std::string response_msg;
  try {
    response_code = stoi(response_code_str);
  } catch (...) {
    response_code = 0;
    ENVOY_LOG(error, "smtp_proxy: error while decoding response code ", response_code);
    data.drain(data.length());
    return SmtpUtils::Result::ProtocolError;
  }

  absl::string_view line_remaining(line);
  line_remaining.remove_prefix(3);
  char sp_or_dash = ' ';
  if (!line_remaining.empty() && line_remaining != CRLF) {
    sp_or_dash = line_remaining[0];
    if (sp_or_dash != ' ' && sp_or_dash != '-') {
      data.drain(data.length());
      return SmtpUtils::Result::ProtocolError;
    }
    line_remaining.remove_prefix(1);
    absl::StrAppend(&response_msg, line_remaining);
  }

  result = session_->handleResponse(response_code, response_msg);
  data.drain(data.length());
  return result;

  /*
  std::string response = data.toString();
  if (!absl::EndsWith(response, SmtpUtils::smtpCrlfSuffix)) {
    result = SmtpUtils::Result::NeedMoreData;
    return result;
  }

  response = StringUtil::cropRight(response, SmtpUtils::smtpCrlfSuffix);
  int length = response.length();
  if (length < 3) {
    // Minimum 3 byte response code needed to parse response from server.
    data.drain(data.length());
    return result;
  }
  std::string response_code_str = response.substr(0, 3);
  uint16_t response_code = 0;
  try {
    response_code = stoi(response_code_str);
  } catch (...) {
    response_code = 0;
    ENVOY_LOG(error, "smtp_proxy: error while decoding response code ", response_code);
  }
  if (response_code >= 400 && response_code < 500) {
    callbacks_->inc4xxErrors();
  } else if (response_code >= 500 && response_code <= 599) {
    callbacks_->inc5xxErrors();
  }
  // Special handling to parse any error response to connection request.
  if (!(session_->isCommandInProgress()) &&
      session_->getState() != SmtpSession::State::ConnectionRequest) {
    data.drain(data.length());
    return result;
  }
 */

}

} // namespace SmtpProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
#pragma once
#include <string>

namespace duckdb {

struct HttpResponse {
	int status_code = 0;
	std::string body;
	std::string error;
	bool ok() const { return status_code >= 200 && status_code < 300; }
};

// Simple synchronous HTTP client (POSIX sockets, no extra deps)
class HttpSQLHttpClient {
public:
	explicit HttpSQLHttpClient(const std::string &base_url);

	HttpResponse Get(const std::string &path);
	HttpResponse Post(const std::string &path, const std::string &body);

private:
	std::string host_;
	int port_ = 80;
	std::string base_path_;

	HttpResponse DoRequest(const std::string &method, const std::string &path,
	                       const std::string &body);
};

} // namespace duckdb

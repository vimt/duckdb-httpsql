#pragma once
#include <mutex>
#include <string>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>

namespace duckdb {

struct HttpResponse {
	int status_code = 0;
	std::string body;
	std::string error;
	bool ok() const { return status_code >= 200 && status_code < 300; }
};

// Synchronous HTTP/1.1 client with persistent connection pool.
// DNS is resolved once at construction. Idle connections are reused across
// requests. On failure the request is retried on a fresh connection.
class HttpSQLHttpClient {
public:
	explicit HttpSQLHttpClient(const std::string &base_url);
	~HttpSQLHttpClient();

	HttpResponse Get(const std::string &path);
	HttpResponse Post(const std::string &path, const std::string &body);

private:
	std::string host_;
	int port_ = 80;

	// Resolved once in constructor
	struct sockaddr_storage server_addr_ {};
	socklen_t server_addr_len_ = 0;
	int addr_family_ = 0;
	int sock_proto_ = 0;
	bool addr_resolved_ = false;

	// Idle connection pool
	std::mutex pool_mutex_;
	std::vector<int> idle_conns_;
	static constexpr int kMaxIdleConns = 8;

	int NewConn();
	int AcquireConn();
	void ReleaseConn(int fd);
	void CloseConn(int fd);

	HttpResponse DoRequest(const std::string &method, const std::string &path,
	                       const std::string &body);
	HttpResponse DoRequestOnFd(int fd, const std::string &method, const std::string &path,
	                           const std::string &body);
};

} // namespace duckdb

#pragma once
#include <mutex>
#include <memory>
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

class HttpSQLHttpClient; // forward declaration

// ─── Streaming HTTP response body reader ──────────────────────────────────────
//
// Reads the HTTP response body incrementally from the socket.
// Transparently decodes both chunked transfer-encoding and content-length
// responses into a flat byte stream.
//
// Owns the socket fd: returns it to the client's connection pool when the body
// is read cleanly to the end; closes it on error or early destruction.
class HttpSQLBodyStream {
public:
	HttpSQLBodyStream(HttpSQLHttpClient *client, int fd,
	                  const char *buf_data, size_t buf_start, size_t buf_end,
	                  bool is_chunked, int64_t content_length);
	~HttpSQLBodyStream();

	HttpSQLBodyStream(const HttpSQLBodyStream &) = delete;
	HttpSQLBodyStream &operator=(const HttpSQLBodyStream &) = delete;

	// Read exactly n bytes into dst. Returns false on EOF or error.
	bool ReadExact(void *dst, size_t n);

	bool Finished() const { return finished_; }
	const std::string &LastError() const { return error_; }

private:
	HttpSQLHttpClient *client_;
	int fd_;
	char buf_[8192];
	size_t buf_start_, buf_end_;
	bool buf_eof_ = false;

	bool is_chunked_;
	int64_t chunk_remaining_ = 0;    // bytes remaining in current chunk (chunked mode)
	int64_t content_remaining_ = -1; // bytes remaining total (content-length mode; -1 = unknown)
	bool finished_ = false;
	std::string error_;

	bool FillBuf();
	bool ReadByte(char &c);
	bool ReadLine(std::string &line);
	bool AdvanceChunk(); // read next chunk-size line
	bool ReadRaw(void *dst, size_t n);
};

struct HttpStreamingResponse {
	int status_code = 0;
	std::unique_ptr<HttpSQLBodyStream> body;
	std::string error;
	bool ok() const { return status_code >= 200 && status_code < 300; }
};

// ─── HTTP client ──────────────────────────────────────────────────────────────
//
// Synchronous HTTP/1.1 client with persistent connection pool.
//
// Supported URL formats:
//   http://host:port[/path]          — TCP connection
//   http+unix:///abs/path/to.sock    — Unix domain socket
//
// DNS is resolved once at construction. Idle connections are reused across
// requests. On failure the request is retried on a fresh connection.
class HttpSQLHttpClient {
public:
	// timeout_sec: SO_RCVTIMEO / SO_SNDTIMEO on each socket. 0 = no timeout.
	explicit HttpSQLHttpClient(const std::string &base_url, int timeout_sec = 30);
	~HttpSQLHttpClient();

	HttpResponse Get(const std::string &path);
	HttpResponse Post(const std::string &path, const std::string &body);

	// Like Post but returns a streaming body reader instead of buffering the
	// entire response. On non-2xx status the error body is read into
	// HttpStreamingResponse::error and body is left null.
	HttpStreamingResponse PostStreaming(const std::string &path, const std::string &body);

	// Called by HttpSQLBodyStream on destruction.
	void ReleaseConn(int fd);
	void CloseConn(int fd);

private:
	bool is_unix_ = false;
	std::string unix_path_; // used when is_unix_ == true

	std::string host_;
	int port_ = 80;
	int timeout_sec_ = 30;

	// Resolved once in constructor (TCP only)
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

	HttpResponse DoRequest(const std::string &method, const std::string &path,
	                       const std::string &body);
	HttpResponse DoRequestOnFd(int fd, const std::string &method, const std::string &path,
	                           const std::string &body);
	HttpStreamingResponse DoStreamingPostOnFd(int fd, const std::string &path,
	                                          const std::string &body);
};

} // namespace duckdb

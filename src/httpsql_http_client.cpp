#include "httpsql_http_client.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cctype>
#include <cstring>
#include <stdexcept>
#include <string>

namespace duckdb {

// ─── URL parsing ──────────────────────────────────────────────────────────────

// Returns true if url is http+unix scheme, and sets unix_path.
// http+unix:///var/run/httpsql.sock  →  unix_path = "/var/run/httpsql.sock"
static bool ParseUnixURL(const std::string &url, std::string &unix_path) {
	const std::string prefix = "http+unix://";
	if (url.substr(0, prefix.size()) != prefix) return false;
	// Everything after "http+unix://" up to '?' is the socket path.
	std::string rest = url.substr(prefix.size());
	auto qpos = rest.find('?');
	unix_path = (qpos != std::string::npos) ? rest.substr(0, qpos) : rest;
	return true;
}

static void ParseURL(const std::string &url, std::string &host, int &port) {
	std::string u = url;
	if (u.substr(0, 7) == "http://") u = u.substr(7);
	auto qpos = u.find('?');
	if (qpos != std::string::npos) u = u.substr(0, qpos);
	auto slash = u.find('/');
	std::string hostport = (slash != std::string::npos) ? u.substr(0, slash) : u;
	auto colon = hostport.find(':');
	if (colon != std::string::npos) {
		host = hostport.substr(0, colon);
		port = std::stoi(hostport.substr(colon + 1));
	} else {
		host = hostport;
		port = 80;
	}
}

// ─── Buffered reader (avoids one-syscall-per-byte) ───────────────────────────

struct ConnReader {
	int fd;
	char buf[8192];
	size_t start = 0, end = 0;
	bool eof = false;

	bool Fill() {
		ssize_t n = recv(fd, buf, sizeof(buf), 0);
		if (n <= 0) { eof = true; return false; }
		start = 0;
		end = (size_t)n;
		return true;
	}

	bool ReadByte(char &c) {
		if (start >= end && !Fill()) return false;
		c = buf[start++];
		return true;
	}

	// Read until \r\n (consumes \r\n, returns line without it)
	bool ReadLine(std::string &line) {
		line.clear();
		char c;
		while (ReadByte(c)) {
			if (c == '\r') { ReadByte(c); return true; } // consume \n
			line += c;
		}
		return false;
	}

	// Read exactly n bytes into out
	bool ReadExact(std::string &out, size_t n) {
		out.reserve(out.size() + n);
		while (n > 0) {
			if (start >= end && !Fill()) return false;
			size_t avail = std::min(n, end - start);
			out.append(buf + start, avail);
			start += avail;
			n -= avail;
		}
		return true;
	}
};

// ─── Constructor / Destructor ─────────────────────────────────────────────────

HttpSQLHttpClient::HttpSQLHttpClient(const std::string &base_url, int timeout_sec)
    : timeout_sec_(timeout_sec) {
	if (ParseUnixURL(base_url, unix_path_)) {
		is_unix_ = true;
		host_ = "localhost";  // used only in HTTP Host header
	} else {
		ParseURL(base_url, host_, port_);
		struct addrinfo hints {}, *res = nullptr;
		hints.ai_family   = AF_UNSPEC;
		hints.ai_socktype = SOCK_STREAM;
		if (getaddrinfo(host_.c_str(), std::to_string(port_).c_str(), &hints, &res) == 0 && res) {
			memcpy(&server_addr_, res->ai_addr, res->ai_addrlen);
			server_addr_len_ = (socklen_t)res->ai_addrlen;
			addr_family_     = res->ai_family;
			sock_proto_      = res->ai_protocol;
			addr_resolved_   = true;
			freeaddrinfo(res);
		}
	}
}

HttpSQLHttpClient::~HttpSQLHttpClient() {
	std::lock_guard<std::mutex> l(pool_mutex_);
	for (int fd : idle_conns_) close(fd);
	idle_conns_.clear();
}

// ─── Connection pool ──────────────────────────────────────────────────────────

int HttpSQLHttpClient::NewConn() {
	int fd = -1;

	if (is_unix_) {
		fd = socket(AF_UNIX, SOCK_STREAM, 0);
		if (fd < 0) return -1;
		struct sockaddr_un addr {};
		addr.sun_family = AF_UNIX;
		strncpy(addr.sun_path, unix_path_.c_str(), sizeof(addr.sun_path) - 1);
		if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
			close(fd);
			return -1;
		}
	} else {
		if (!addr_resolved_) return -1;
		fd = socket(addr_family_, SOCK_STREAM, sock_proto_);
		if (fd < 0) return -1;
		int one = 1;
		setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
		if (connect(fd, (struct sockaddr *)&server_addr_, server_addr_len_) < 0) {
			close(fd);
			return -1;
		}
	}

	if (timeout_sec_ > 0) {
		struct timeval tv { timeout_sec_, 0 };
		setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
		setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
	}
	return fd;
}

int HttpSQLHttpClient::AcquireConn() {
	{
		std::lock_guard<std::mutex> l(pool_mutex_);
		if (!idle_conns_.empty()) {
			int fd = idle_conns_.back();
			idle_conns_.pop_back();
			return fd;
		}
	}
	return NewConn();
}

void HttpSQLHttpClient::ReleaseConn(int fd) {
	std::lock_guard<std::mutex> l(pool_mutex_);
	if ((int)idle_conns_.size() < kMaxIdleConns) {
		idle_conns_.push_back(fd);
	} else {
		close(fd);
	}
}

void HttpSQLHttpClient::CloseConn(int fd) {
	if (fd >= 0) close(fd);
}

// ─── Request / Response ───────────────────────────────────────────────────────

HttpResponse HttpSQLHttpClient::DoRequestOnFd(int fd, const std::string &method,
                                               const std::string &path,
                                               const std::string &body) {
	HttpResponse resp;

	// Build and send request
	std::string req;
	req.reserve(256 + body.size());
	req += method; req += ' '; req += path; req += " HTTP/1.1\r\n";
	req += "Host: "; req += host_; req += ':'; req += std::to_string(port_); req += "\r\n";
	req += "Connection: keep-alive\r\n";
	if (!body.empty()) {
		req += "Content-Type: application/json\r\n";
		req += "Content-Length: "; req += std::to_string(body.size()); req += "\r\n";
	}
	req += "\r\n";
	req += body;

	// Send all bytes
	size_t sent = 0;
	while (sent < req.size()) {
		ssize_t n = send(fd, req.c_str() + sent, req.size() - sent, MSG_NOSIGNAL);
		if (n <= 0) { resp.error = "send() failed"; return resp; }
		sent += n;
	}

	// Read response headers
	ConnReader reader;
	reader.fd = fd;
	std::string status_line;
	if (!reader.ReadLine(status_line)) { resp.error = "recv() failed on status line"; return resp; }

	// Parse status code: "HTTP/1.1 200 OK"
	auto sp1 = status_line.find(' ');
	if (sp1 != std::string::npos) {
		resp.status_code = std::stoi(status_line.substr(sp1 + 1));
	}

	bool is_chunked = false;
	int64_t content_length = -1;

	std::string header_line;
	while (reader.ReadLine(header_line) && !header_line.empty()) {
		auto colon = header_line.find(':');
		if (colon == std::string::npos) continue;
		std::string key   = header_line.substr(0, colon);
		std::string value = header_line.substr(colon + 1);
		// trim whitespace
		while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) value.erase(0, 1);
		// lowercase key for comparison
		for (auto &c : key) c = (char)tolower((unsigned char)c);
		if (key == "transfer-encoding") {
			for (auto &c : value) c = (char)tolower((unsigned char)c);
			is_chunked = value.find("chunked") != std::string::npos;
		} else if (key == "content-length") {
			content_length = std::stoll(value);
		}
	}

	// Read body
	if (is_chunked) {
		std::string chunk_size_line;
		while (reader.ReadLine(chunk_size_line)) {
			auto semi = chunk_size_line.find(';');
			if (semi != std::string::npos) chunk_size_line = chunk_size_line.substr(0, semi);
			if (chunk_size_line.empty()) continue;
			size_t chunk_size = std::stoul(chunk_size_line, nullptr, 16);
			if (chunk_size == 0) {
				reader.ReadLine(chunk_size_line); // consume trailing \r\n
				break;
			}
			if (!reader.ReadExact(resp.body, chunk_size)) break;
			reader.ReadLine(chunk_size_line); // consume \r\n after chunk data
		}
	} else if (content_length >= 0) {
		reader.ReadExact(resp.body, (size_t)content_length);
	} else {
		// Fallback: read until connection close
		std::string tmp;
		while (reader.ReadExact(tmp, sizeof(reader.buf))) {}
		resp.body += tmp;
	}

	return resp;
}

HttpResponse HttpSQLHttpClient::DoRequest(const std::string &method, const std::string &path,
                                           const std::string &body) {
	// Try up to 2 attempts: first on a pooled/existing conn, then on a fresh one
	for (int attempt = 0; attempt < 2; attempt++) {
		int fd = (attempt == 0) ? AcquireConn() : NewConn();
		if (fd < 0) {
			HttpResponse r;
			r.error = "failed to connect to " + host_ + ":" + std::to_string(port_);
			return r;
		}
		auto resp = DoRequestOnFd(fd, method, path, body);
		if (resp.status_code > 0) {
			ReleaseConn(fd);
			return resp;
		}
		// Connection was stale or broken — discard and retry with a fresh one
		CloseConn(fd);
	}
	HttpResponse r;
	r.error = "connection failed after retry";
	return r;
}

HttpResponse HttpSQLHttpClient::Get(const std::string &path) {
	return DoRequest("GET", path, "");
}

HttpResponse HttpSQLHttpClient::Post(const std::string &path, const std::string &body) {
	return DoRequest("POST", path, body);
}

} // namespace duckdb

#include "httpsql_http_client.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <sstream>
#include <stdexcept>
#include <string>

namespace duckdb {

static void ParseURL(const std::string &url, std::string &host, int &port, std::string &path) {
	std::string u = url;
	if (u.substr(0, 7) == "http://") {
		u = u.substr(7);
	}
	// strip query string from url for base
	auto qpos = u.find('?');
	if (qpos != std::string::npos) {
		u = u.substr(0, qpos);
	}
	auto slash = u.find('/');
	std::string hostport;
	if (slash != std::string::npos) {
		hostport = u.substr(0, slash);
		path = u.substr(slash);
	} else {
		hostport = u;
		path = "/";
	}
	auto colon = hostport.find(':');
	if (colon != std::string::npos) {
		host = hostport.substr(0, colon);
		port = std::stoi(hostport.substr(colon + 1));
	} else {
		host = hostport;
		port = 80;
	}
}

HttpSQLHttpClient::HttpSQLHttpClient(const std::string &base_url) {
	ParseURL(base_url, host_, port_, base_path_);
}

HttpResponse HttpSQLHttpClient::Get(const std::string &path) {
	return DoRequest("GET", path, "");
}

HttpResponse HttpSQLHttpClient::Post(const std::string &path, const std::string &body) {
	return DoRequest("POST", path, body);
}

HttpResponse HttpSQLHttpClient::DoRequest(const std::string &method, const std::string &path,
                                             const std::string &body) {
	HttpResponse resp;

	struct addrinfo hints {}, *res = nullptr;
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	std::string port_str = std::to_string(port_);
	if (getaddrinfo(host_.c_str(), port_str.c_str(), &hints, &res) != 0) {
		resp.error = "DNS lookup failed for " + host_;
		return resp;
	}

	int sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
	if (sock < 0) {
		freeaddrinfo(res);
		resp.error = "socket() failed";
		return resp;
	}

	if (connect(sock, res->ai_addr, res->ai_addrlen) < 0) {
		close(sock);
		freeaddrinfo(res);
		resp.error = "connect() failed to " + host_ + ":" + port_str;
		return resp;
	}
	freeaddrinfo(res);

	std::ostringstream req;
	req << method << " " << path << " HTTP/1.1\r\n";
	req << "Host: " << host_ << ":" << port_ << "\r\n";
	req << "Connection: close\r\n";
	if (!body.empty()) {
		req << "Content-Type: application/json\r\n";
		req << "Content-Length: " << body.size() << "\r\n";
	}
	req << "\r\n";
	if (!body.empty()) {
		req << body;
	}
	std::string req_str = req.str();
	send(sock, req_str.c_str(), req_str.size(), 0);

	// Read response
	std::string raw;
	char buf[4096];
	ssize_t n;
	while ((n = recv(sock, buf, sizeof(buf), 0)) > 0) {
		raw.append(buf, n);
	}
	close(sock);

	// Parse HTTP response
	auto header_end = raw.find("\r\n\r\n");
	if (header_end == std::string::npos) {
		resp.error = "malformed HTTP response";
		return resp;
	}
	std::string headers = raw.substr(0, header_end);
	std::string raw_body = raw.substr(header_end + 4);

	// Parse status code from first line
	auto sp1 = headers.find(' ');
	if (sp1 != std::string::npos) {
		auto sp2 = headers.find(' ', sp1 + 1);
		resp.status_code = std::stoi(headers.substr(sp1 + 1, sp2 - sp1 - 1));
	}

	// Check for chunked transfer encoding
	bool is_chunked = false;
	{
		auto lower_headers = headers;
		for (auto &c : lower_headers) c = (char)tolower((unsigned char)c);
		is_chunked = lower_headers.find("transfer-encoding: chunked") != std::string::npos;
	}

	if (is_chunked) {
		// Decode chunked body
		std::string decoded;
		size_t pos = 0;
		while (pos < raw_body.size()) {
			auto nl = raw_body.find("\r\n", pos);
			if (nl == std::string::npos) break;
			std::string size_str = raw_body.substr(pos, nl - pos);
			// Remove chunk extensions
			auto semi = size_str.find(';');
			if (semi != std::string::npos) size_str = size_str.substr(0, semi);
			if (size_str.empty()) break;
			size_t chunk_size = std::stoul(size_str, nullptr, 16);
			if (chunk_size == 0) break;
			pos = nl + 2;
			if (pos + chunk_size > raw_body.size()) {
				decoded.append(raw_body, pos, raw_body.size() - pos);
				break;
			}
			decoded.append(raw_body, pos, chunk_size);
			pos += chunk_size + 2; // skip data + \r\n
		}
		resp.body = std::move(decoded);
	} else {
		resp.body = std::move(raw_body);
	}

	return resp;
}

} // namespace duckdb

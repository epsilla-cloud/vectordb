#include "server/fulltext/implementations/quickwit/quickwit_client.hpp"

#include <curl/curl.h>
#include <sstream>

namespace vectordb {
namespace server {
namespace fulltext {
namespace quickwit {

// cURL 回调函数
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
  ((std::string*)userp)->append((char*)contents, size * nmemb);
  return size * nmemb;
}

QuickwitClient::QuickwitClient(const std::string& base_url)
  : base_url_(base_url) {

  // 初始化 curl（全局初始化，只需一次）
  static bool curl_initialized = false;
  if (!curl_initialized) {
    curl_global_init(CURL_GLOBAL_ALL);
    curl_initialized = true;
  }

  logger_.Info("[Quickwit] Client initialized [endpoint=" + base_url + "]");
}

std::pair<int, std::string> QuickwitClient::HttpGet(const std::string& path) {
  CURL* curl = curl_easy_init();
  std::string response_body;
  int http_code = 0;

  if (curl) {
    std::string url = base_url_ + path;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

    CURLcode res = curl_easy_perform(curl);

    if (res == CURLE_OK) {
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    } else {
      logger_.Error("[Quickwit] HTTP GET failed [error=" + std::string(curl_easy_strerror(res)) + "]");
      http_code = 0;
    }

    curl_easy_cleanup(curl);
  }

  return {http_code, response_body};
}

std::pair<int, std::string> QuickwitClient::HttpPost(const std::string& path,
                                                       const std::string& body,
                                                       const std::string& content_type) {
  CURL* curl = curl_easy_init();
  std::string response_body;
  int http_code = 0;

  if (curl) {
    std::string url = base_url_ + path;

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, ("Content-Type: " + content_type).c_str());

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, body.size());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

    CURLcode res = curl_easy_perform(curl);

    if (res == CURLE_OK) {
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    } else {
      logger_.Error("[Quickwit] HTTP POST failed [error=" + std::string(curl_easy_strerror(res)) + "]");
      http_code = 0;
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }

  return {http_code, response_body};
}

std::pair<int, std::string> QuickwitClient::HttpDelete(const std::string& path) {
  CURL* curl = curl_easy_init();
  std::string response_body;
  int http_code = 0;

  if (curl) {
    std::string url = base_url_ + path;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

    CURLcode res = curl_easy_perform(curl);

    if (res == CURLE_OK) {
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    } else {
      logger_.Error("[Quickwit] HTTP DELETE failed [error=" + std::string(curl_easy_strerror(res)) + "]");
      http_code = 0;
    }

    curl_easy_cleanup(curl);
  }

  return {http_code, response_body};
}

Status QuickwitClient::CreateIndex(const std::string& index_id,
                                     const std::string& index_config) {
  auto [code, body] = HttpPost("/api/v1/indexes", index_config, "application/yaml");

  if (code == 200 || code == 201) {
    logger_.Info("[Quickwit] Index created [index=" + index_id + "]");
    return Status::OK();
  } else {
    logger_.Error("[Quickwit] Failed to create index [index=" + index_id +
                  ", http_code=" + std::to_string(code) + ", error=" + body + "]");
    return Status(DB_UNEXPECTED_ERROR, "Failed to create index: HTTP " + std::to_string(code));
  }
}

Status QuickwitClient::DeleteIndex(const std::string& index_id) {
  auto [code, body] = HttpDelete("/api/v1/indexes/" + index_id);

  if (code == 200) {
    logger_.Info("[Quickwit] Index deleted [index=" + index_id + "]");
    return Status::OK();
  } else {
    logger_.Error("[Quickwit] Failed to delete index [index=" + index_id +
                  ", http_code=" + std::to_string(code) + "]");
    return Status(DB_UNEXPECTED_ERROR, "Failed to delete index: HTTP " + std::to_string(code));
  }
}

bool QuickwitClient::IndexExists(const std::string& index_id) {
  auto [code, body] = HttpGet("/api/v1/indexes/" + index_id);
  return code == 200;
}

Status QuickwitClient::IndexDocument(const std::string& index_id,
                                       const Json& document) {
  return IndexBatch(index_id, {document});
}

Status QuickwitClient::IndexBatch(const std::string& index_id,
                                    const std::vector<Json>& documents) {
  if (documents.empty()) {
    return Status::OK();
  }

  // 转换为 NDJSON 格式
  std::string ndjson = ToNDJSON(documents);

  auto [code, body] = HttpPost("/api/v1/" + index_id + "/ingest",
                                 ndjson,
                                 "application/x-ndjson");

  if (code == 200) {
    logger_.Debug("[Quickwit] Indexed documents [index=" + index_id +
                  ", count=" + std::to_string(documents.size()) + "]");
    return Status::OK();
  } else {
    logger_.Error("[Quickwit] Failed to index documents [http_code=" +
                  std::to_string(code) + ", error=" + body + "]");
    return Status(DB_UNEXPECTED_ERROR, "Failed to index documents: HTTP " + std::to_string(code));
  }
}

Status QuickwitClient::DeleteByQuery(const std::string& index_id,
                                       const std::string& query) {
  Json request;
  request.SetString("query", query);

  auto [code, body] = HttpPost("/api/v1/" + index_id + "/delete-tasks",
                                 request.DumpToString());

  if (code == 200) {
    logger_.Info("[Quickwit] Delete task created [query=" + query + "]");
    return Status::OK();
  } else {
    logger_.Error("[Quickwit] Failed to create delete task [http_code=" +
                  std::to_string(code) + "]");
    return Status(DB_UNEXPECTED_ERROR, "Failed to delete: HTTP " + std::to_string(code));
  }
}

Status QuickwitClient::DeleteByIds(const std::string& index_id,
                                     const std::vector<std::string>& ids) {
  if (ids.empty()) {
    return Status::OK();
  }

  // 构造 IN 查询: id IN ["id1", "id2", ...]
  std::string query = "id IN [";
  for (size_t i = 0; i < ids.size(); ++i) {
    query += "\"" + ids[i] + "\"";
    if (i < ids.size() - 1) {
      query += ",";
    }
  }
  query += "]";

  return DeleteByQuery(index_id, query);
}

Json QuickwitClient::Search(const std::string& index_id,
                              const std::string& query,
                              size_t max_hits,
                              size_t start_offset,
                              const std::vector<std::string>& search_fields) {
  Json request;
  request.SetString("query", query);
  request.SetInt("max_hits", max_hits);
  request.SetInt("start_offset", start_offset);

  if (!search_fields.empty()) {
    // TODO: 添加 search_fields 支持
  }

  return SearchAdvanced(index_id, request);
}

Json QuickwitClient::SearchAdvanced(const std::string& index_id,
                                      const Json& search_request) {
  Json request_copy = search_request;  // Copy to call non-const method
  auto [code, body] = HttpPost("/api/v1/" + index_id + "/search",
                                 request_copy.DumpToString());

  if (code == 200) {
    try {
      Json result;
      if (result.LoadFromString(body)) {
        return result;
      } else {
        logger_.Error("[Quickwit] Failed to parse search response");
        return Json();
      }
    } catch (...) {
      logger_.Error("[Quickwit] Failed to parse search response [exception]");
      return Json();
    }
  } else {
    logger_.Error("[Quickwit] Search failed [http_code=" + std::to_string(code) + "]");
    return Json();
  }
}

bool QuickwitClient::HealthCheck() {
  auto [code, body] = HttpGet("/health/ready");
  return code == 200;
}

std::string QuickwitClient::ToNDJSON(const std::vector<Json>& documents) {
  std::string ndjson;
  for (auto doc : documents) {  // Copy to call non-const method
    ndjson += doc.DumpToString() + "\n";
  }
  return ndjson;
}

} // namespace quickwit
} // namespace fulltext
} // namespace server
} // namespace vectordb

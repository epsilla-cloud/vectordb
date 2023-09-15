#include <oatpp/web/server/interceptor/RequestInterceptor.hpp>

#include "logger/logger.hpp"

class EpsillaRequestInterceptor : public oatpp::web::server::interceptor::RequestInterceptor {
public:
  EpsillaRequestInterceptor() {}

  std::shared_ptr<vectordb::engine::Logger> logger = std::make_shared<vectordb::engine::Logger>();

  std::shared_ptr<OutgoingResponse> intercept(const std::shared_ptr<IncomingRequest>& request) override {
    // Get the full request path using the helper function
    std::string full_path = request->getStartingLine().path.toString();

    // Log the full request path
    logger->Info(full_path);

    // Return nullptr to continue processing the request
    return nullptr;
  }

};
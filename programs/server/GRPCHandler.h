#include <iostream>
#include <memory>
#include <string>
#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>
#include "helloworld.grpc.pb.h"

using HelloApi::HelloRequest;
using HelloApi::HelloResponse;
using HelloApi::Hello;

class GRPCHandler final : public Hello::Service {
  grpc::Status SayHello(grpc::ServerContext* context, const HelloRequest* request,
                  HelloResponse* reply) override {
    reply->set_username("ClikHouse");
    return grpc::Status::OK;
  }
};

#include <iostream>
#include <memory>
#include <string>

#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/security/credentials.h>
#include "helloworld.grpc.pb.h"

class GRPCClient {
 public:
  GRPCClient(std::shared_ptr<grpc::Channel> channel)
      : stub_(HelloApi::Hello::NewStub(channel)) {}

  std::string SayHello(const std::string& username) {
    HelloApi::HelloRequest request;
    request.set_username(username);
    HelloApi::HelloResponse reply;
    grpc::ClientContext context;

    grpc::Status status = stub_->SayHello(&context, request, &reply);
    if (status.ok()) {
      return reply.username();
    } else {
      return "none";
    }
  }

 private:
  std::unique_ptr<HelloApi::Hello::Stub> stub_;
};

int main(int argc, char** argv) {
  std::cout << "Try: " << argv[1] << std::endl;
  GRPCClient client(
    grpc::CreateChannel(argv[1], grpc::InsecureChannelCredentials()));
  std::string user("anonim");
  std::string reply = client.SayHello(user);
  std::cout << "Received: " << reply << std::endl;
  
  return 0;
}

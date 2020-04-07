
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
get_filename_component(rpc_proto "${CMAKE_CURRENT_SOURCE_DIR}/../server/helloworld.proto" ABSOLUTE)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${rpc_proto})
PROTOBUF_GENERATE_GRPC_CPP(GRPC_SRCS GRPC_HDRS ${rpc_proto})

find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

add_executable(grpc-client client.cpp ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS})
target_link_libraries(grpc-client gRPC::grpc++)
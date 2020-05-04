# coding: utf-8

import os
import pytest
import subprocess
import grpc
from docker.models.containers import Container

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
proto_dir = os.path.join(SCRIPT_DIR, './protos')
subprocess.check_call(
    'python -m grpc_tools.protoc -I{proto_path} --python_out=. --grpc_python_out=. \
    {proto_path}/GrpcConnection.proto'.format(proto_path=proto_dir), shell=True)

import GrpcConnection_pb2
import GrpcConnection_pb2_grpc

config_dir = os.path.join(SCRIPT_DIR, './configs')
cluster = ClickHouseCluster(__file__)

node = cluster.add_instance('node', config_dir=config_dir, env_variables={'UBSAN_OPTIONS': 'print_stacktrace=1'})
server_port = 9001

@pytest.fixture(scope="module")
def server_address():
    cluster.start()
    try:
        yield cluster.get_instance_ip('node')
    finally:
        cluster.shutdown()
def Query(server_address_and_port, query):
    output = []
    with grpc.insecure_channel(server_address_and_port) as channel:
        stub = GrpcConnection_pb2_grpc.GRPCStub(channel)
        user_info = GrpcConnection_pb2.User(user="default", key='123', quota='default')
        query_info = GrpcConnection_pb2.QuerySettings(query=query, query_id='123', format="TabSeparated")
        for response in stub.Query(GrpcConnection_pb2.QueryRequest(user_info=user_info, query_info=query_info, interactive_delay=1000)):
            output += response.query.split()
    return output

def test_ordinary_query(server_address):
    server_address_and_port = server_address + ':' + str(server_port)
    assert Query(server_address_and_port, "SELECT 1") == [u'1']
    assert Query(server_address_and_port, "SELECT count() FROM numbers(100)") == [u'100']

def test_query_insert(server_address):
    server_address_and_port = server_address + ':' + str(server_port)
    assert Query(server_address_and_port, "CREATE TABLE t (a UInt8) ENGINE = Memory") == []
    assert Query(server_address_and_port, "INSERT INTO t VALUES (1),(2),(3)") == []
    assert Query(server_address_and_port, "INSERT INTO t FORMAT TabSeparated 10\n11\n12\n") == []
    assert Query(server_address_and_port, "SELECT a FROM t ORDER BY a") == [u'1', u'2', u'3', u'10', u'11', u'12']
    assert Query(server_address_and_port, "DROP TABLE t") == []

def test_handle_mistakes(server_address):
    server_address_and_port = server_address + ':' + str(server_port)
    assert Query(server_address_and_port, "") == []
    assert Query(server_address_and_port, "CREATE TABLE t (a UInt8) ENGINE = Memory") == []
    assert Query(server_address_and_port, "CREATE TABLE t (a UInt8) ENGINE = Memory") == []
    




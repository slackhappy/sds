.PHONY: proto

proto: sds_pb2_grpc.py

sds_pb2_grpc.py sds_pb2.py sds_pb2.pyi: protos/sds.proto
	@echo "building protos"
	.venv/bin/python -m  grpc_tools.protoc --proto_path=./protos/  --python_out=. --pyi_out=. --grpc_python_out=. protos/sds.proto
import sds_pb2
import sds_pb2_grpc
import grpc
import logging
import wikipedia_views_pb2
from google.protobuf.message import Message
from typing import TypeVar, Type, Dict, List

T = TypeVar('T', bound=Message)


class SDSClient:
    def __init__(self, host_port: str):
        self.channel = grpc.insecure_channel(host_port)
        self.stub = sds_pb2_grpc.StaticDataServiceStub(self.channel)

    def __del__(self):
         self.channel.close()
    
    def get(self, keys: List[str], value_type: Type[T]) -> Dict[str, T]:
        keys = ['Main_Page|en', 'Main_Page|de']
        keys_rev = {idx: key for idx, key in enumerate(keys)}
        res = self.stub.GetValuesSingle(sds_pb2.GetRequest(table='dbs/test.db', keys=keys))
        res_map = {}
        for idx, v in res.values.items():
            res_map[keys_rev[idx]] = value_type.FromString(v)
        return res_map

def run():
        sds = SDSClient("localhost:50051") 
        keys = ['Main_Page|en', 'Main_Page|de']
        res = sds.get(keys, wikipedia_views_pb2.WikipediaViews)
        print(res)

if __name__ == "__main__":
    logging.basicConfig()
    run()
# Copyright 2024 John Gallagher
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from concurrent import futures
import logging
import sqlite3
import sys
import os

import grpc
import sds_pb2
import sds_pb2_grpc

DB_SEARCH_PATHS=[]
DATABASES={}

class Database:
    def __init__(self, path):
        self.path = path

    def connect(self):
        return sqlite3.connect(self.path)

class StaticDataServiceImpl(sds_pb2_grpc.StaticDataServiceServicer):
    """Provides methods that implement functionality of route guide server."""

    def __init__(self):
        pass

    def GetValuesSingle(self, request: sds_pb2.GetRequest, context):
        table = request.table
        if table not in DATABASES:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Table not found')
            raise IndexError('Table not found - error')
        con = DATABASES[table].connect()
        cur = con.cursor()
        values = {}
        for idx, k in enumerate(request.keys):
            rs = cur.execute("select value from data where key=?", (k,))
            for row in rs:
                values[idx] = row[0]
        return sds_pb2.GetResponse(values=values)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    sds_pb2_grpc.add_StaticDataServiceServicer_to_server(
        StaticDataServiceImpl(), server
    )
    server.add_insecure_port("[::]:50051")
    server.start()

    server.wait_for_termination()

def refresh_dbs():
    print('refresh')
    found_dbs = set()
    current_dbs = DATABASES.keys()


    print(DB_SEARCH_PATHS)
    for root in DB_SEARCH_PATHS:
        print(root)
        for path, _, files in os.walk(root):
            print(files)
            for file in files:
                if file.endswith('.db'):
                    db_path = os.path.join(path, file)
                    print('found db', db_path)
                    found_dbs.add(db_path)

    for db in found_dbs - current_dbs:
        print('added db', db)
        DATABASES[db] = Database(db)
    
    for db in current_dbs - found_dbs:
        del DATABASES[db]


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('Usage:', sys.argv[0], '<path-to-dbs>')
        sys.exit(1)
    DB_SEARCH_PATHS.append(sys.argv[1])
    refresh_dbs()
    logging.basicConfig()
    serve()
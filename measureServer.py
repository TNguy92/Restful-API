import json
import sqlite3
import logging
import re

from http.server import HTTPServer, BaseHTTPRequestHandler
logging.basicConfig(format='%(asctime)s %(message)s',
                    level=logging.DEBUG)
def dictionary_factory(cursor, row):
    col_names = [d[0].lower() for d in cursor.description]
    return dict(zip(col_names, row))

class RestHandler(BaseHTTPRequestHandler):

    def get_data(self):
        if 'content-length' in self.headers:
            data = self.rfile.read(int(self.headers['content-length'])).decode()
            logging.info("data is " + data)
            return json.loads(data)
        else:
            logging.debug('no content length')
            return {}

    def send_data(self, rtdata):
        self.send_response(200)
        self.send_header("content-type", "application/json")
        rt = json.dumps(rtdata).encode()
        self.send_header('content-length', len(rt))
        self.end_headers()
        self.wfile.write(rt)

    def send_error_message(self, message, status_code=412):
        self.send_response(status_code)
        self.send_header("content-type", "application/json")
        rt = json.dumps({'error': message}).encode()
        self.send_header('content-length', len(rt))
        self.end_headers()
        self.wfile.write(rt)

    def do_GET(self):
        connection = sqlite3.connect("measures.sqlite")
        connection.row_factory = dictionary_factory
        cursor = connection.cursor()
        if re.fullmatch("/area", self.path):
            cursor.execute("SELECT * FROM area")
            data = cursor.fetchall()
            self.send_data(data)

        elif re.fullmatch("/area/(\\d+)/location", self.path):
            match = re.fullmatch("/area/(\\d+)/location", self.path)
            id = int(match.group(1))
            cursor.execute("SELECT * From location WHERE location_area = ?",[id])
            data = cursor.fetchall()
            self.send_data(data)

        elif re.fullmatch("/location/(\d+)/measurement",self.path):
            match = re.fullmatch("/location/(\d+)/measurement",self.path)
            id = int(match.group(1))
            cursor.execute("SELECT * From measurement WHERE  measurement_location = ? ",[id])
            data = cursor.fetchall()
            self.send_data(data)

        elif re.fullmatch("/area/(\d+)/category",self.path):
            match = re.fullmatch("/area/(\d+)/category",self.path)
            id = int(match.group(1))
            cursor.execute("SELECT * From category_area LEFT OUTER JOIN category ON category_area.category_id = category.category_id WHERE area_id = ?",[id])
            data = cursor.fetchall()
            self.send_data(data)

        elif re.fullmatch("/area/(\d+)/average_measurement", self.path):
            match = re.fullmatch("/area/(\d+)/average_measurement", self.path)
            id = int(match.group(1))
            cursor.execute("SELECT AVG(value) From measurement LEFT OUTER JOIN location on measurement.measurement_location = location.location_id WHERE location_area = ?",[id])
            data = cursor.fetchone()
            self.send_data(data['avg(value)'])
        elif re.fullmatch("/area/(\d+)/number_locations", self.path):
            match = re.fullmatch("/area/(\d+)/number_locations", self.path)
            id = int(match.group(1))
            cursor.execute("SELECT count(location_area) From location WHERE location_area = ? ", [id])
            data = cursor.fetchone()
            self.send_data(data['count(location_area)'])
        else:
            cursor.close()
            connection.close()

server_port = 12345


if __name__ == '__main__':
    server_address = ('', server_port)
    httpd = HTTPServer(server_address, RestHandler)
    httpd.serve_forever()
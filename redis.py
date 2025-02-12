# client logic and server skeleton
import gevent

from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer

from collections import namedtuple
from io import BytesIO
from socket import error as socket_error


# exceptions for connection handling
class CommandError(Exception): pass
class Disconnect(Exception): pass

Error = namedtuple('Error', ('message'))

class ProtocolHandler(object):
    def __init__(self):
        self.handlers = {
            '+': self.handle_simple_string,
            '-': self.handle_error,
            ':': self.handle_integer,
            '$': self.handle_string,
            '*': self.handle_array,
            '%': self.handle_dict}
        
    def handle_request(self, socket_file):
        # Parsing request from clients
        first_byte = socket_file.read(1)
        if not first_byte:
            raise Disconnect()
        
        try:
            # Delegate to the appropriate handler 
            return self.handlers[first_byte](socket_file)
        except KeyError:
            raise CommandError('bad protocol')
        
    def handle_simple_string(self, socket_file):
        return socket_file.readline().strip('\r\n')
        
    def handle_error(self, socket_file):
        return Error(socket_file.readline().strip('\r\n'))
        
    def handle_integer(self, socket_file):
        return int(socket_file.readline().strip('\r\n')) 
        
    def handle_string(self, socket_file):
        # first read the length ($<length>\r\n)
        length = int(socket_file.readline().strip('\r\n'))
        if length == -1:
            return None # in case of nulls
        length += 2 # include the trailing \r\n count
        return socket_file.read(length)[:-2]
        
    def handle_array(self, socket_file):
        num_items = int(socket_file.readline().rstrip('\r\n'))
        elements = [self.handle_request(socket_file) for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline().rstrip('\r\n'))
        elements = [self.handle_request(socket_file) for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))
    
    def write_response(self, socket_file, data):
        buf = BytesIO()
        self.write(buf, data)
        buf.seek(0)
        socket_file.write(buf.getvalue())
        socket_file.flush()
        
    def write(self, buf, data):
        if isinstance(data,str):
            data = data.encode('utf-8')
            
        if isinstance(data, bytes):
            buf.write('$%s\r\n%s\r\n' % (len(data), data))
        elif isinstance(data, int):
            buf.write(':%s\r\n' % data)
        elif isinstance(data, Error):
            buf.write('-%s\r\n' % data.message)
        elif isinstance(data, (list, tuple)):
            buf.write('*%s\r\n' % len(data))
            for item in data:
                self.write(buf, item)
        elif isinstance(data, dict):
            buf.write('%%%s\r\n' % len(data))
            for key in data:
                self.write(buf, key)
                self.write(buf, data[key])
        elif data is None:
            buf.write('$-1\r\n')
        else:
            raise CommandError('unrecognized type: %s ' % type(data))
    
class Server(object):
    def __init__(self, host: str ='127.0.0.1', port: int = 31337, max_clients: int=64 ):
        self.pool = Pool(max_clients)
        self._server = StreamServer(
            (host, port),
            self.connection_handler,
            spawn = self.pool)
    
        self._protocol = ProtocolHandler()
        self._kv = {}
        
        self._commands = self.get_commands()
        
    def get_commands(self):
        return {
            'GET': self.get,
            'SET': self.set,
            'DELETE': self.delete,
            'FLUSH': self.flush,
            'MGET': self.mget,
            'MSET': self.mset}
        
    def get_response(self, data):
        if not isinstance(data, list):
            try: 
                data = data.split()
            except: 
                raise CommandError('request must list or simpe string')
            
        if not data:
            raise CommandError('wrong/missing command')
        
        command  = data[0].upper()
        if command not in self._commands:
            raise CommandError('Unrecognized command: %s' % command)
    
        return self._commands[command](*data[1:]) 
    
    def get(self, key):
        return self._kv.get(key)

    def set(self, key, value):
        self._kv[key] = value
        return 1

    def delete(self, key):
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0

    def flush(self):
        kvlen = len(self._kv)
        self._kv.clear()
        return kvlen

    def mget(self, *keys):
        return [self._kv.get(key) for key in keys]

    def mset(self, *items):
        data = zip(items[::2], items[1::2])
        for key, value in data:
            self._kv[key] = value
        return len(data) 
         
    def connection_handler(self, conn, address):
        #convert conn into a file like object 
        socket_file = conn.makefile('rwb')
        
        #process client requests until client disconnection
        while True:
            try:
                data = self._protocol.handle_request(socket_file)
            
            except Disconnect:
                break
            
            try:
                resp= self.get_response(data)
                
            except CommandError as exception:
                resp = Error(exception.args[0])
                
            self._protocol.write_response(socket_file, resp)
    
    def run(self):
        self._server.serve_forever()
            
class Client(object):
    def __init__(self, host='127.0.0.1', port=31337):
         self._protocol = ProtocolHandler()
         self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
         self._socket.connect((host, port))
         self._fh = self._socket.makefile('rwb')

    def execute(self, *args):
        self._protocol.write_response(self._fh, args)
        resp = self._protocol.handle_request(self._fh)
        if isinstance(resp, Error):
                raise CommandError(resp.message)
        return resp
    
    def get(self, key):
        return self.execute('GET', key)

    def set(self, key, value):
        return self.execute('SET', key, value)

    def delete(self, key):
        return self.execute('DELETE', key)

    def flush(self):
        return self.execute('FLUSH')

    def mget(self, *keys):
        return self.execute('MGET', *keys)

    def mset(self, *items):
        return self.execute('MSET', *items)
    
if __name__== '__main__': 
    from gevent import monkey; monkey.patch_all()
    Server().run()

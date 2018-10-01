from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import struct
import re

RESPONSE_CODES = {
    0: 'No error',
    1: 'Key not found',
    2: 'Key exists',
    3: 'Value too large',
    4: 'Invalid arguments',
    5: 'Item not stored',
    6: 'Incr/decr on a non-numeric value',
    7: 'The vbucket belongs to another server',
    8: 'Authentication error',
    9: 'Authentication continue',
    0x20: 'Authentication required',
    0x81: 'Unknown command',
    0x82: 'Out of memory',
    0x83: 'Not supported',
    0x84: 'Internal error',
    0x85: 'Busy',
    0x86: 'Temporary failure',
}

OPCODES = {
    'get': 0x00,
    'set': 0x01,
    'add': 0x02,
    'replace': 0x03,
    'delete': 0x04,
    'incr': 0x05,
    'decr': 0x06,
    'flush': 0x08,
    'noop': 0x0A,
    'version': 0x0B,
    'getkq': 0x0D,
    'append': 0x0E,
    'prepend': 0x0F,
    'stat': 0x10,
    'setq': 0x11,
    'addq': 0x12,
    'replaceq': 0x13,
    'deleteq': 0x14,
    'incrq': 0x15,
    'decrq': 0x16,
    'auth_negotiation': 0x20,
    'auth_request': 0x21,
    'auth_continue': 0x22,
    'touch': 0x1C,
}

OP_FORMAT = {
    # 'set': 'IIss',
    # 'add': 'NNa*a*',
    # 'replace': 'NNa*a*',
    # 'delete': 'a*',
    # 'incr': 'NNNNNa*',
    # 'decr': 'NNNNNa*',
    # 'flush': 'N',
    # 'noop': '',
    # 'getkq': 'a*',
    # 'version': '',
    # 'stat': 'a*',
    # 'append': 'a*a*',
    # 'prepend': 'a*a*',
    # 'auth_request': 'a*a*',
    # 'auth_continue': 'a*a*',
    # 'touch': 'Na*',
}

HEADER = ">BBHBBHIIL"
CAS_HEADER = "@4CCnNNQ"
# NORMAL_HEADER = '@4CCnN'
# KV_HEADER = '@2n@6nN@16Q'

REQUEST = 0x80
RESPONSE = 0x81

# TODO: quiet operations
class MCBinaryProtocol:
    def get(self, keys, return_all_info=False):
        key_size = _bytesize(key)
        buffer = struct.pack(HEADER, REQUEST, OPCODES['get'], key_size, 0, 0, 0, key_size, 0, 0) + key.encode("utf-8")
        connection.sendall(buffer)
        return self._generic_response(connection)

    def gets(self, keys):
        buffer = struct.pack(FORMAT['getkq'], REQUEST, OPCODES['getkq'], _bytesize(key), 0, 0, 0, _bytesize(key), 0, 0, key)
        return self.connection.sendall(buffer)

    def metaget(self, keys):
        pass

    def leaseGet(self, keys):
        pass

    def leaseSet(self, key, value_token, exptime=0, is_stalestored=False):
        pass

    def set(self, connection, key, value, replicate=False, noreply=False, exptime=0, flags=0):
        cas = 0 # TODO: cas support
        key_size = _bytesize(key)
        value_size = _bytesize(value)
        # import pdb; pdb.set_trace()
        buffer = struct.pack(HEADER + "II", REQUEST, OPCODES['set'], key_size, 8, 0, 0, value_size + key_size + 8, 0, cas, flags, exptime) + key.encode("utf-8") + value.encode("utf-8")
        connection.sendall(buffer)
        return self._cas_response(connection)

    def add(self, connection, key, value, replicate=False, noreply=False, exptime=0, flags=0):
        key_size = _bytesize(key)
        value_size = _bytesize(value)
        buffer = struct.pack(HEADER + "II", REQUEST, OPCODES['add'],  key_size, 8, 0, 0, value_size + key_size + 8, 0, 0, flags, ttl) + key.encode("utf-8") + value.encode("utf-8")
        return self.connection.sendall(buffer)

    def replace(self, connection, key, value, replicate=False, noreply=False, exptime=0, flags=0):
        cas = 0 # TODO: cas support
        key_size = _bytesize(key)
        value_size = _bytesize(value)
        buffer = struct.pack(HEADER + "II", REQUEST, OPCODES['replace'], key_size, 8, 0, 0, value_size + key_size + 8, 0, cas, flags, exptime) + key.encode("utf-8") + value.encode("utf-8")
        return self.connection.sendall(buffer)

    def delete(self, connection, key, exptime=None, noreply=False):
        cas = 0 # TODO: cas support
        key_size = _bytesize(key)
        buffer = struct.pack(FORMAT['delete'], REQUEST, OPCODES['delete'], key_size, 0, 0, 0, key_size, 0, cas, key)
        return self.connection.sendall(buffer)

    def touch(self, connection, key, exptime, noreply=False):
        key_size = _bytesize(key)
        buffer = struct.pack(FORMAT['touch'], REQUEST, key_size, 4, 0, 0, key_size + 4, 0, 0, exptime,key)
        return self.connection.sendall(buffer)

    def _arith(self, connection, cmd, key, value, noreply, exptime):
        buffer = struct.pack(FORMAT[cmd], REQUEST, OPCODES[cmd], _bytesize(key), 20, 0, 0, _bytesize(key) + 20, 0, 0, h, l, dh, dl, exptime, key)
        return self.connection.sendall(buffer)

    def incr(self, connection, key, value=1, noreply=False, exptime=0):
        return _arith('incr', key, value, noreply, exptime)

    def decr(self, connection, key, value=1, noreply=False):
        return _arith('decr', key, value, noreply, exptime)

    def _affix(self, connection, cmd, key, value, noreply, flags, exptime):
        buffer = struct.pack(FORMAT[cmd], REQUEST, OPCODES[cmd], _bytesize(key), 0, 0, 0, _bytesize(value) + _bytesize(key), 0, 0, key, value)
        pass

    def append(self, connection, key, value, noreply=False, flags=0, exptime=0):
        return _affix('append', key, value, noreply, flags, exptime)

    def prepend(self, connection, key, value, noreply=False, flags=0, exptime=0):
        return _affix('prepend', key, value, noreply, flags, exptime)

    def cas(self, connection, key, value, cas_token):
        pass

    def stats(self, connection, spec=None):
        buffer = struct.pack(FORMAT['stats'], REQUEST, OPCODES['stats'], _bytesize(spec), 0, 0, 0, _bytesize(spec), 0, 0, spec)
        return self.connection.sendall(buffer)

    def raw_stats(self, spec=None):
        pass

    def issue_command_and_read_all(self, connection, command):
        raise NotImplementedError('not supported for binary protocol')

    def issue_command(self, connection, command):
        raise NotImplementedError('not supported for binary protocol')

    def version(self, connection):
        buffer = struct.pack(FORMAT['noop'], REQUEST, OPCODES['version'], 0, 0, 0, 0, 0, 0, 0)
        return connection.sendall(buffer)

    def shutdown(self):
        raise NotImplementedError('TODO(maximebedard)')

    def flush_all(self, connection, delay=None):
        buffer = struct.pack(FORMAT['flush'], REQUEST, OPCODES['flush'], 0, 4, 0, 0, 4, 0, 0, 0)
        return connection.sendall(buffer)

    def _cas_response(self, connection):
        header = connection.read(24)
        if not header:
            raise Exception("invalid header {}".format(header.encode("utf-8")))

        (_, _, status, count, _, cas) = struct.unpack(CAS_HEADER, header)
        if count > 0:
            connection.read(count) # skip potential data that we don't care about

        if status == 1:
            return None
        elif status == 2 or status == 5:
            return False
        elif status != 0:
            raise Exception("response error")
        else:
            return cas

    def _generic_response(self, connection):
        header = connection.read(24)
        if not header:
            raise Exception("invalid header {}".format(header.encode("utf-8")))

        (extras, _, status, count) = struct.unpack(NORMAL_HEADER, header)

        data = None
        if count > 0:
            data = read(count)

        if status == 1:
            raise Exception("not found")
        elif status == 2 or status == 5:
            return False # Not stored, normal status for add operation
        elif status != 0:
            raise Exception("response error")
        elif data:
            return 0
            # TODO:
            #     flags = data[0...extras].unpack('N')[0]
            #     value = data[extras..-1]
            #     unpack ? deserialize(value, flags) : value
        else:
            return True

def _bytesize(s):
    return len(s.encode("utf-8"))

def _split(n):
    return (n >> 32, 0xFFFFFF & n)

class MCAsciiProtocol:
    def _get(self, connection, cmd, keys, expect_cas, return_all_info):
        multi = True
        hadValue = False
        if not isinstance(keys, list):
            multi = False
            keys = [keys]
        connection.sendall("{} {}\r\n".format(cmd, " ".join(keys)))
        res = dict([(key, None) for key in keys])

        while True:
            l = connection.readline().strip()
            if l == 'END':
                if multi:
                    return res
                else:
                    assert len(res) == 1
                    return res.values()[0]
            elif l.startswith("VALUE"):
                hadValue = True
                parts = l.split()
                k = parts[1]
                f = int(parts[2])
                n = int(parts[3])
                assert k in keys
                payload = connection.read(n)
                connection.read(2)
                if return_all_info:
                    res[k] = dict({"key": k,
                                  "flags": f,
                                  "size": n,
                                  "value": payload})
                    if expect_cas:
                        res[k]["cas"] = int(parts[4])
                else:
                    res[k] = payload
            elif l.startswith("SERVER_ERROR"):
                if hadValue:
                    raise Exception('Received hit reply + SERVER_ERROR for '
                                    'multiget request')
                return l
            else:
                connection.connect()
                raise Exception('Unexpected response "%s" (%s)' % (l, keys))

    def get(self, connection, keys, return_all_info=False):
        return self._get(connection, 'get', keys, expect_cas=False, return_all_info=return_all_info)

    def gets(self, connection, keys):
        return self._get(connection, 'gets', keys, expect_cas=True, return_all_info=True)

    def metaget(self, connection, keys):
        ## FIXME: Not supporting multi-metaget yet
        #multi = True
        #if not instance(keys, list):
        #    multi = False
        #    keys = [keys]
        res = {}
        connection.sendall("metaget %s\r\n" % keys)

        while True:
            l = connection.readline().strip()
            if l.startswith("END"):
                return res
            elif l.startswith("META"):
                meta_list = l.split()
                for i in range(1, len(meta_list) // 2):
                    res[meta_list[2 * i].strip(':')] = \
                        meta_list[2 * i + 1].strip(';')

    def leaseGet(self, connection, keys):
        multi = True
        if not isinstance(keys, list):
            multi = False
            keys = [keys]
        connection.sendall("lease-get %s\r\n" % " ".join(keys))
        res = dict([(key, None) for key in keys])

        while True:
            l = connection.readline().strip()
            if l == 'END':
                if multi:
                    assert(len(res) == len(keys))
                    return res
                else:
                    assert len(res) == 1
                    return res.values()[0]
            elif l.startswith("VALUE"):
                v, k, f, n = l.split()
                assert k in keys
                res[k] = {"value": self.fd.read(int(n)),
                          "token": None}
                self.fd.read(2)
            elif l.startswith("LVALUE"):
                v, k, t, f, n = l.split()
                assert k in keys
                res[k] = {"value": self.fd.read(int(n)),
                          "token": int(t)}

    def _set(self, connection, command, key, value, replicate=False, noreply=False, exptime=0, flags=0):
        value = str(value)
        flags = flags | (1024 if replicate else 0)
        connection.sendall("%s %s %d %d %d%s\r\n%s\r\n" %
                            (command, key, flags, exptime, len(value),
                            (' noreply' if noreply else ''), value))
        if noreply:
            return connection.expectNoReply()

        answer = connection.readline().strip()
        if re.search('ERROR', answer):
            print(answer)
            connection.connect()
            return None
        return re.match("STORED", answer)

    def leaseSet(self, key, value_token, exptime=0, is_stalestored=False):
        value = str(value_token["value"])
        token = int(value_token["token"])
        flags = 0
        cmd = "lease-set %s %d %d %d %d\r\n%s\r\n" % \
                (key, token, flags, exptime, len(value), value)
        connection.sendall(cmd)

        answer = connection.readline().strip()
        if re.search('ERROR', answer):
            print(answer)
            connection.connect()
            return None
        if is_stalestored:
            return re.match("STALE_STORED", answer)
        return re.match("STORED", answer)

    def set(self, connection, key, value, replicate=False, noreply=False, exptime=0, flags=0):
        return self._set(connection, "set", key, value, replicate, noreply, exptime, flags)

    def add(self, connection, key, value, replicate=False, noreply=False):
        return self._set(connection, "add", key, value, replicate, noreply)

    def replace(self, connection, key, value, replicate=False, noreply=False):
        return self._set(connection, "replace", key, value, replicate, noreply)

    def delete(self, connection, key, exptime=None, noreply=False):
        exptime_str = ''
        if exptime is not None:
            exptime_str = " {}".format(exptime)
        connection.sendall("delete {}{}{}\r\n".format(
                            key, exptime_str, (' noreply' if noreply else '')))

        if noreply:
            return connection.expectNoReply()

        answer = connection.readline()

        assert re.match("DELETED|NOT_FOUND|SERVER_ERROR", answer), answer
        return re.match("DELETED", answer)

    def touch(self, connection, key, exptime, noreply=False):
        connection.sendall("touch {} {}{}\r\n".format(
                            key, exptime, (' noreply' if noreply else '')))

        if noreply:
            return connection.expectNoReply()

        answer = connection.readline()

        if answer == "TOUCHED\r\n":
            return "TOUCHED"
        if answer == "NOT_FOUND\r\n":
            return "NOT_FOUND"
        if re.match("^SERVER_ERROR", answer):
            return "SERVER_ERROR"
        if re.match("^CLIENT_ERROR", answer):
            return "CLIENT_ERROR"
        return None

    def _arith(self, connection, cmd, key, value, noreply):
        connection.sendall("%s %s %d%s\r\n" %
                            (cmd, key, value, (' noreply' if noreply else '')))
        if noreply:
            return connection.expectNoReply()

        answer = connection.readline()
        if re.match("NOT_FOUND", answer):
            return None
        else:
            return int(answer)

    def incr(self, connection, key, value=1, noreply=False):
        return self._arith('incr', key, value, noreply)

    def decr(self, connection, key, value=1, noreply=False):
        return self._arith('decr', key, value, noreply)

    def _affix(self, connection, cmd, key, value, noreply=False, flags=0, exptime=0):
        connection.sendall("%s %s %d %d %d%s\r\n%s\r\n" %
                            (cmd, key, flags, exptime, len(value),
                            (' noreply' if noreply else ''), value))

        if noreply:
            return connection.expectNoReply()

        answer = connection.readline()
        if answer == "STORED\r\n":
            return "STORED"
        if answer == "NOT_STORED\r\n":
            return "NOT_STORED"
        if re.match("^SERVER_ERROR", answer):
            return "SERVER_ERROR"
        if re.match("^CLIENT_ERROR", answer):
            return "CLIENT_ERROR"
        return None

    def append(self, connection, key, value, noreply=False, flags=0, exptime=0):
        return self._affix(connection, 'append', key, value, noreply, flags, exptime)

    def prepend(self, connection, key, value, noreply=False, flags=0, exptime=0):
        return self._affix(connection, 'prepend', key, value, noreply, flags, exptime)

    def cas(self, connection, key, value, cas_token):
        value = str(value)
        connection.sendall("cas %s 0 0 %d %d\r\n%s\r\n" %
                            (key, len(value), cas_token, value))

        answer = connection.readline().strip()
        if re.search('ERROR', answer):
            print(answer)
            connection.connect()
            return None
        return re.match("STORED", answer)

    def stats(self, connection, spec=None):
        q = 'stats\r\n'
        if spec:
            q = 'stats {0}\r\n'.format(spec)
        connection.sendall(q)

        s = {}
        l = None
        if connection.no_response(5.0):
            return None

        while l != 'END':
            l = connection.readline().strip()
            if len(l) == 0:
                return None
            a = l.split(None, 2)
            if len(a) == 3:
                s[a[1]] = a[2]

        return s

    def raw_stats(self, connection, spec=None):
        q = 'stats\r\n'
        if spec:
            q = 'stats {0}\r\n'.format(spec)
        connection.sendall(q)

        s = []
        l = None
        if connection.no_response(2.0):
            return None

        while l != 'END':
            l = connection.readline().strip()
            a = l.split(None, 1)
            if len(a) == 2:
                s.append(a[1])

        return s

    def issue_command_and_read_all(self, connection, command):
        connection.sendall(command)

        if connection.no_response(2.0):
            return None

        answer = ""
        l = None
        while l != 'END':
            l = connection.readline().strip()
            # Handle error
            if not answer and 'ERROR' in l:
                connection.connect()
                return l
            answer += l + "\r\n"
        return answer

    def issue_command(self, connection, command):
        connection.sendall(command)
        return connection.readline()

    def version(self, connection):
        connection.sendall("version\r\n")
        return connection.readline()

    def shutdown(self, connection):
        connection.sendall("shutdown\r\n")
        return connection.readline()

    def flush_all(self, connection, delay=None):
        if delay is None:
            connection.sendall("flush_all\r\n")
        else:
            connection.sendall("flush_all {}\r\n".format(delay))
        return connection.readline().rstrip()


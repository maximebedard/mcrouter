# Copyright (c) 2017-present, Facebook, Inc.
#
# This source code is licensed under the MIT license found in the LICENSE
# file in the root directory of this source tree.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import errno
import os
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

from mcrouter.test.config import McrouterGlobals
from mcrouter.test.MCProtocol import MCAsciiProtocol

class BaseDirectory(object):
    def __init__(self, prefix="mctest"):
        self.path = tempfile.mkdtemp(prefix=prefix + '.')

    def __del__(self):
        shutil.rmtree(self.path)

def MCPopen(cmd, stdout=None, stderr=None, env=None):
    return subprocess.Popen(cmd, stdout=stdout, stderr=stderr, env=env)

class ProcessBase(object):
    """
    Generic process, extended by mcrouter, memcached, mcpiper, etc
    """

    proc = None

    def __init__(self, cmd, base_dir=None, junk_fill=False):
        if base_dir is None:
            base_dir = BaseDirectory('ProcessBase')
        self.base_dir = base_dir
        self.stdout = os.path.join(base_dir.path, 'stdout')
        self.stderr = os.path.join(base_dir.path, 'stderr') 
        print(os.path.join(base_dir.path, 'mcrouter.log'))
	stdout = open(self.stdout, 'w')
        stderr = open(self.stderr, 'w')

        self.cmd_line = 'no command line'
        if cmd:
            self.cmd_line = ' '.join(cmd)
            for command in cmd:
                if command == 'python':
                    continue
                if command.startswith('-'):
                    continue
                command = os.path.basename(command)
                break

            try:
                if junk_fill:
                    env = dict(MALLOC_CONF='junk:true')
                else:
                    env = None
                self.proc = MCPopen(cmd, stdout, stderr, env)

            except OSError:
                sys.exit("Fatal: Could not run " + repr(" ".join(cmd)))
        else:
            self.proc = None

    def __del__(self):
        if self.proc:
            self.proc.terminate()

    def getprocess(self):
        return self.proc

    def pause(self):
        if self.proc:
            self.proc.send_signal(signal.SIGSTOP)

    def resume(self):
        if self.proc:
            self.proc.send_signal(signal.SIGCONT)

    def is_alive(self):
        self.proc.poll()
        return self.proc.returncode is None

    def dump(self):
        """ dump stderr, stdout, and the log file to stdout with nice headers.
        This allows us to get all this information in a test failure (hidden by
        default) so we can debug better. """

        # Grumble... this would be so much easier if I could just pass
        # sys.stdout/stderr to Popen.
        with open(self.stdout, 'r') as stdout_f:
            stdout = stdout_f.read()

        with open(self.stderr, 'r') as stderr_f:
            stderr = stderr_f.read()

        if hasattr(self, 'log'):
            print(self.base_dir)
            try:
                with open(self.log, 'r') as log_f:
                    log = log_f.read()
            except IOError:
                log = ""
        else:
            log = ""

        if log:
            print("{} ({}) stdout:\n{}".format(self, self.cmd_line, log))
        if stdout:
            print("{} ({}) stdout:\n{}".format(self, self.cmd_line, stdout))
        if stderr:
            print("{} ({}) stdout:\n{}".format(self, self.cmd_line, stderr))

class MCProcess(ProcessBase):
    proc = None

    def __init__(self, cmd, addr, base_dir=None, junk_fill=False, protocol=MCAsciiProtocol()):
        if cmd is not None and '-s' in cmd:
            if os.path.exists(addr):
                raise Exception('file path already existed')
            self.addr = addr
            self.port = 0
            self.addr_family = socket.AF_UNIX
        else:
            port = int(addr)
            self.addr = ('localhost', port)
            self.port = port
            self.addr_family = socket.AF_INET

        if base_dir is None:
            base_dir = BaseDirectory('MCProcess')

        self.protocol = protocol

        ProcessBase.__init__(self, cmd, base_dir, junk_fill)
        self.deletes = 0
        self.others = 0

    def getport(self):
        return self.port

    def connect(self):
        self.socket = socket.socket(self.addr_family, socket.SOCK_STREAM)
        self.socket.connect(self.addr)
        self.fd = self.socket.makefile()

    def ensure_connected(self):
        while True:
            try:
                self.connect()
                return
            except Exception as e:
                if e.errno == errno.ECONNREFUSED:
                    pass
                else:
                    raise

    def disconnect(self):
        try:
            if self.socket:
                self.socket.close()
        except IOError:
            pass
        try:
            if self.fd:
                self.fd.close()
        except IOError:
            pass
        self.fd = self.socket = None

    def terminate(self):
        if not self.proc:
            return None

        if hasattr(self, 'socket'):
            self.disconnect()

        self.dump()

        proc = self.proc
        if self.proc:
            if self.proc.returncode is None:
                self.proc.terminate()
            self.proc.wait()
            self.proc = None

        return proc

    def expectNoReply(self):
        self.socket.settimeout(0.5)
        try:
            self.socket.recv(1)
            return False
        except socket.timeout:
            pass
        return True

    def sendall(self, *args):
        return self.socket.sendall(*args)

    def readline(self):
        return self.fd.readline()

    def read(self, *args):
        return self.fd.read(*args)

    def no_response(self, timeout):
        fds = select.select([self.fd], [], [], timeout)
        return len(fds[0]) == 0

    def get(self, *args, **kwargs):
        return self.protocol.get(self, *args, **kwargs)

    def gets(self, *args, **kwargs):
        return self.protocol.gets(self, *args, **kwargs)

    def metaget(self, *args, **kwargs):
        return self.protocol.metaget(self, *args, **kwargs)

    def leaseGet(self, *args, **kwargs):
        return self.protocol.leaseGet(self, *args, **kwargs)

    def leaseSet(self, *args, **kwargs):
        return self.protocol.leaseSet(self, *args, **kwargs)

    def set(self, *args, **kwargs):
        return self.protocol.set(self, *args, **kwargs)

    def add(self, *args, **kwargs):
        return self.protocol.add(self, *args, **kwargs)

    def replace(self, *args, **kwargs):
        return self.protocol.replace(self, *args, **kwargs)

    def delete(self, *args, **kwargs):
        self.deletes += 1
        return self.protocol.delete(self, *args, **kwargs)

    def touch(self, *args, **kwargs):
        return self.protocol.touch(self, *args, **kwargs)

    def incr(self, *args, **kwargs):
        return self.protocol.incr(self, *args, **kwargs)

    def decr(self, *args, **kwargs):
        return self.protocol.decr(self, *args, **kwargs)

    def append(self, *args, **kwargs):
        return self.protocol.append(self, *args, **kwargs)

    def prepend(self, *args, **kwargs):
        return self.protocol.prepend(self, *args, **kwargs)

    def cas(self, *args, **kwargs):
        return self.protocol.cas(self, *args, **kwargs)

    def stats(self, *args, **kwargs):
        return self.protocol.stats(self, *args, **kwargs)

    def raw_stats(self, *args, **kwargs):
        return self.protocol.raw_stats(self, *args, **kwargs)

    def issue_command_and_read_all(self, *args, **kwargs):
        self.others += 1
        return self.protocol.issue_command_and_read_all(self, *args, **kwargs)

    def issue_command(self, *args, **kwargs):
        self.others += 1
        return self.protocol.issue_command(self, *args, **kwargs)

    def version(self):
        return self.protocol.version(self, *args, **kwargs)

    def shutdown(self):
        return self.protocol.shutdown(self, *args, **kwargs)

    def flush_all(self, *args, **kwargs):
        return self.protocol.flush_all(self, *args, **kwargs)

def sub_port(s, substitute_ports, port_map):
    parts = s.split(':')
    if len(parts) < 2:
        return s

    for i in (-1, -2):
        try:
            port = int(parts[i])
            if port not in port_map:
                if len(port_map) < len(substitute_ports):
                    if isinstance(substitute_ports, list):
                        port_map[port] = substitute_ports[len(port_map)]
                    else:
                        if port not in substitute_ports:
                            raise Exception(
                                "Port %s not in substitute port map" % port)
                        port_map[port] = substitute_ports[port]
                else:
                    raise Exception("Looking up port %d: config file has more"
                                    " ports specified than the number of"
                                    " mock servers started" % port)
            parts[i] = str(port_map[port])
        except (IndexError, ValueError):
            pass
    return ':'.join(parts)


def replace_ports(json, substitute_ports):
    """In string json (which must be a valid JSON string), replace all ports in
    strings of the form "host:port" with ports from the list or map
    substitute_ports.

    If list, each new distinct port from the json will be replaced from the
    next port from the list.

    If map of the form (old_port: new_port), replaces all old_ports with
    new_ports.
    """
    NORMAL = 0
    STRING = 1
    ESCAPE = 2

    state = NORMAL
    out = ""
    s = ""
    port_map = {}
    for c in json:
        if state == NORMAL:
            out += c
            if c == '"':
                s = ""
                state = STRING
        elif state == STRING:
            if c == '\\':
                s += c
                state = ESCAPE
            elif c == '"':
                out += sub_port(s, substitute_ports, port_map)
                out += c
                state = NORMAL
            else:
                s += c
        elif state == ESCAPE:
            s += c
            state = NORMAL

    if len(port_map) < len(substitute_ports):
        raise Exception("Config file has fewer ports specified than the number"
                        " of mock servers started")
    return out

def replace_strings(json, replace_map):
    for (key, value) in replace_map.items():
        json = json.replace(key, str(value))
    return json

def create_listen_socket():
    if socket.has_ipv6:
        listen_sock = socket.socket(socket.AF_INET6)
    else:
        listen_sock = socket.socket(socket.AF_INET)
    listen_sock.listen(100)
    return listen_sock

class McrouterBase(MCProcess):
    def __init__(self, args, port=None, base_dir=None, protocol=MCAsciiProtocol()):
        if base_dir is None:
            base_dir = BaseDirectory('mcrouter')

        self.log = os.path.join(base_dir.path, 'mcrouter.log')

        self.async_spool = os.path.join(base_dir.path, 'spool.mcrouter')
        os.mkdir(self.async_spool)
        self.stats_dir = os.path.join(base_dir.path, 'stats')
        os.mkdir(self.stats_dir)
        self.debug_fifo_root = os.path.join(base_dir.path, 'fifos')
        os.mkdir(self.debug_fifo_root)

        args.extend(['-L', self.log,
                     '-a', self.async_spool,
                     '--stats-root', self.stats_dir,
                     '--debug-fifo-root', self.debug_fifo_root])

        listen_sock = None
        if port is None:
            listen_sock = create_listen_socket()
            port = listen_sock.getsockname()[1]
            args.extend(['--listen-sock-fd', str(listen_sock.fileno())])
        else:
            args.extend(['-p', str(port)])

        args = McrouterGlobals.preprocessArgs(args)

        MCProcess.__init__(self, args, port, base_dir, junk_fill=True, protocol=protocol)

        if listen_sock is not None:
            listen_sock.close()

    def get_async_spool_dir(self):
        return self.async_spool

    def check_in_log(self, needle):
        return needle in open(self.log).read()


class Mcrouter(McrouterBase):
    def __init__(self, config, port=None, default_route=None, extra_args=None,
                 base_dir=None, substitute_config_ports=None,
                 substitute_port_map=None, replace_map=None, protocol=MCAsciiProtocol()):
        if base_dir is None:
            base_dir = BaseDirectory('mcrouter')

        if replace_map:
            with open(config, 'r') as config_file:
                replaced_config = replace_strings(config_file.read(),
                                                  replace_map)
            (_, config) = tempfile.mkstemp(dir=base_dir.path)
            with open(config, 'w') as config_file:
                config_file.write(replaced_config)

        if substitute_config_ports:
            with open(config, 'r') as config_file:
                replaced_config = replace_ports(config_file.read(),
                                                substitute_config_ports)
            (_, config) = tempfile.mkstemp(dir=base_dir.path)
            with open(config, 'w') as config_file:
                config_file.write(replaced_config)

        self.config = config
        args = [McrouterGlobals.binPath('mcrouter'),
                '--config', 'file:' + config]

        if default_route:
            args.extend(['-R', default_route])

        if extra_args:
            args.extend(extra_args)

        if '-b' in args:
            def get_pid():
                stats = self.stats()
                if stats:
                    return int(stats['pid'])
                return None

            def terminate():
                pid = get_pid()
                if pid:
                    os.kill(pid, signal.SIGTERM)

            def is_alive():
                pid = get_pid()
                if pid:
                    return os.path.exists("/proc/{}".format(pid))
                return False

            self.terminate = terminate
            self.is_alive = is_alive

        McrouterBase.__init__(self, args, port, base_dir, protocol=protocol)

    def change_config(self, new_config_path):
        shutil.copyfile(new_config_path, self.config)


class McrouterClient(MCProcess):
    def __init__(self, port):
        MCProcess.__init__(self, None, str(port))

class McrouterClients:
    def __init__(self, port, count):
        self.clients = []
        for i in range(0, count):
            self.clients.append(McrouterClient(port))
            self.clients[i].connect()

    def __getitem__(self, idx):
        return self.clients[idx]

class MockMemcached(MCProcess):
    def __init__(self, port=None):
        args = [McrouterGlobals.binPath('mockmc')]
        listen_sock = None
        if port is None:
            listen_sock = create_listen_socket()
            port = listen_sock.getsockname()[1]
            args.extend(['-t', str(listen_sock.fileno())])
        else:
            args.extend(['-P', str(port)])

        MCProcess.__init__(self, args, port)

        if listen_sock is not None:
            listen_sock.close()

class Memcached(MCProcess):
    def __init__(self, port=None):
        args = [McrouterGlobals.binPath('prodmc')]
        listen_sock = None

        # if mockmc is used here, we initialize the same way as MockMemcached
        if McrouterGlobals.binPath('mockmc') == args[0]:
            if port is None:
                listen_sock = create_listen_socket()
                port = listen_sock.getsockname()[1]
                args.extend(['-t', str(listen_sock.fileno())])
            else:
                args.extend(['-P', str(port)])

            MCProcess.__init__(self, args, port)

            if listen_sock is not None:
                listen_sock.close()
        else:
            args.extend([
                '-A',
                '-g',
                '-t', '1',
                '--enable-hash-alias',
                '--enable-unchecked-l1-sentinel-reads',
                '--use-asmcs',
                '--reaper_throttle=100',
                '--ini-hashpower=16',
            ])
            if port is None:
                listen_sock = create_listen_socket()
                port = listen_sock.getsockname()[1]
                args.extend(['--listen-sock-fd', str(listen_sock.fileno())])
            else:
                args.extend(['-p', str(port)])

            MCProcess.__init__(self, args, port)

            if listen_sock is not None:
                listen_sock.close()

            # delay here until the server goes up
            self.ensure_connected()
            l = 10
            s = self.stats()
            while ((not s or len(s) == 0) and l > 0):
                # Note, we need to reconnect, because it's possible the
                # server is still going to process previous requests.
                self.ensure_connected()
                s = self.stats()
                time.sleep(0.5)
                l = l - 1
            self.disconnect()

class Mcpiper(ProcessBase):
    def __init__(self, fifos_dir, extra_args=None):
        base_dir = BaseDirectory('mcpiper')
        args = [McrouterGlobals.binPath('mcpiper'), '--fifo-root', fifos_dir]

        if extra_args:
            args.extend(extra_args)

        ProcessBase.__init__(self, args, base_dir)

    def output(self):
        with open(self.stdout, 'r') as stdout_f:
            return stdout_f.read().decode('ascii', errors='ignore')

    def contains(self, needle):
        return needle in self.output()

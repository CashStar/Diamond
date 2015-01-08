# coding=utf-8

"""
The UDPCollector class collects metrics on UDP stats (surprise!)

#### Dependencies

 * /proc/net/snmp

"""

import diamond.collector
import os
import socket
import json

class UWSGICollector(diamond.collector.Collector):

    TYPE_MAP = {
        'socket_path': str,
    }

    CORE_METRICS = [
        'listen_queue',
    ]

    WORKER_METRICS = [
        'requests',
        'harakiri_count'
    ]

    def __init__(self, config, handlers):
        super(UWSGICollector, self).__init__(config, handlers)
        self.sockets = {}
        for app_name, cfg in self.config['sockets'].items():
            q_cfg = {}
            for key in ['socket_path',]:
                q_cfg[key] = cfg.get(key, [])
                if not isinstance(q_cfg[key], self.TYPE_MAP[key]):
                    q_cfg[key] = self.TYPE_MAP[key](q_cfg[key])
            self.sockets[app_name] = q_cfg

    def get_default_config_help(self):
        config_help = super(UWSGICollector, self).get_default_config_help()
        config_help.update({
            'sockets': 'A list of sockets to read stats from'
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(UWSGICollector, self).get_default_config()
        config.update({
            'path':             'uwsgi',
            'sockets': {},
        })
        return config

    def read_socket(self, socket_path):
        if os.path.exists( socket_path ):
            try:
                client = socket.socket( socket.AF_UNIX, socket.SOCK_STREAM )
                client.connect( socket_path )
                res = ""
                while True:
                    datagram = client.recv( 1024 )
                    if not datagram:
                        break
                    res += datagram
                return res
            except:
                pass
        return None

    def collect(self):

        for name, args in self.sockets.iteritems():
            socket_path = args.get('socket_path')
            if not os.access(socket_path, os.R_OK):
                self.log.error('Permission to access {0} denied'.format(socket_path))
                continue
            stats = self.read_socket(socket_path)
            if not stats:
                self.log.error('No stats read from {0}'.format(socket_path))
                continue
            try:
                stats = json.loads(stats)
            except:
                self.log.error('Failed to parse stats from {0}'.format(socket_path))
                continue

            base_key = "{0}".format(name)

            for metric in self.CORE_METRICS:
                key = '{0}.{1}'.format(base_key, metric)
                value = stats.get(metric)
                self.publish(key, value)

            for worker in stats.get("workers", []):
                for metric in self.WORKER_METRICS:
                    key = "{0}.{1}.{2}".format(
                        base_key,
                        worker.get("id"),
                        metric
                    )
                    value = worker.get(metric, 0)
                    value = self.derivative(key, long(value))
                    self.publish(key, value)

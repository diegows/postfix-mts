
import sys
sys.path.append('postfixmts/lib')

import os
import sys
import ConfigParser

from postfix_log_parser import PostfixLogParser
from postfixmts.model.postfixlog import *
from postfixmts.model import init_model

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine


config = ConfigParser.ConfigParser( { 'here' : os.getcwd() } )
config.read(os.path.join(os.getcwd(), 'development.ini'))
sqlalchemy_url = config.get('app:main', 'sqlalchemy.url')
engine = create_engine(sqlalchemy_url, echo = False)
init_model(engine)


class PostfixLog2DB(PostfixLogParser):
    def __init__(self):
        pass

    def process_log_prefix(self, log_line, obj):
        obj.date = log_line[self.DATE]
        obj.time = log_line[self.TIME]
        obj.hostname = log_line[self.HOSTNAME]
        if self.QUEUE_ID in log_line:
            obj.queue_id = log_line[self.QUEUE_ID]

    def process_client_line(self, log_line):
        client_line = ClientLine()
        self.process_log_prefix(log_line, client_line)
        client_hostname = log_line[self.CLIENT_HOSTNAME]
        if client_hostname == 'unknown':
            client_line.client_hostname = None
        else:
            client_line.client_hostname = client_hostname
        client_line.client_ip_addr = log_line[self.CLIENT_IP_ADDR]
        try:
            client_line.sasl_method = SaslMethod.get(log_line[self.SASL_METHOD])
            client_line.sasl_username = SaslMethod.get(log_line[self.USERNAME])
        except KeyError:
            pass

        DBSession.add(client_line)
        DBSession.flush()

    def process_msg_id_line(self, log_line):
        msg_id_line = MsgIdLine()
        self.process_log_prefix(log_line, msg_id_line)
        msg_id_line.msg_id = log_line[self.MESSAGE_ID]

        DBSession.add(msg_id_line)
        DBSession.flush()

    def process_from_line(self, log_line):
        from_line = FromLine()
        self.process_log_prefix(log_line, from_line)
        from_line.sender = MailAddress.get(log_line[self.SENDER])
        from_line.size = int(log_line[self.SIZE])
        from_line.nrcpt = int(log_line[self.NRCPT])

        DBSession.add(from_line)
        DBSession.flush()

    def process_sent_line(self, log_line):
        sent_line = SentLine()
        self.process_log_prefix(log_line, sent_line)
        sent_line.recipient = MailAddress.get(log_line[self.RECIPIENT])

        if self.ORIG_TO in log_line:
            sent_line.orig_to = MailAddress.get(log_line[self.ORIG_TO])

        if self.RELAY_HOSTNAME in log_line:
            sent_line.relay_hostname = log_line[self.RELAY_HOSTNAME]
            sent_line.relay_ip_port = log_line[self.RELAY_IP]
            if self.RELAY_PORT in log_line[self.RELAY_PORT]:
                sent_line.relay_ip_port += ':%d' % (log_line[self.RELAY_PORT])
        else:
            sent_line.relay_hostname = log_line[self.RELAY_TRANSPORT]

        sent_line.delay = float(log_line[self.DELAY])
        sent_line.delays = '%s/%s/%s/%s' % tuple(log_line[self.DELAYS])
        sent_line.status = Status.get(log_line[self.STATUS])
        sent_line.status_msg = log_line[self.STATUS_MSG]

        DBSession.add(sent_line)
        DBSession.flush()

    def process_host_fail_line(self, log_line):
        host_fail_line = HostFailLine()
        self.process_log_prefix(log_line, host_fail_line)
        host_fail_line.queue_id = log_line[self.QUEUE_ID]
        host_fail_line.msg = log_line[self.HOST_FAIL_MSG]

        DBSession.add(host_fail_line)
        DBSession.flush()

    def process_reject_line(self, log_line):
        reject_line = RejectLine()
        self.process_log_prefix(log_line, reject_line)
        if log_line[self.CLIENT_HOSTNAME] == 'unknown':
            reject_line.client_hostname = None
        else:
            reject_line.client_hostname = log_line[self.CLIENT_HOSTNAME]
        reject_line.client_ip_addr = log_line[self.CLIENT_IP_ADDR]
        reject_line.error_code = int(log_line[self.ERROR_CODE])
        reject_line.dsn = log_line[self.DSN]
        reject_line.mail_addr = MailAddress.get(log_line[self.MAIL_ADDR])
        reject_line.error_msg = log_line[self.ERROR_MSG]
        reject_line.sender = MailAddress.get(log_line[self.SENDER])
        reject_line.recipient = MailAddress.get(log_line[self.RECIPIENT])

        DBSession.add(reject_line)
        DBSession.flush()


metadata = DeclarativeBase.metadata
metadata.drop_all(engine)
metadata.create_all(engine)

postfix_log_db = PostfixLog2DB()

while True:
    line = sys.stdin.readline()
    if not line:
        break
    postfix_log_db.parse(line)


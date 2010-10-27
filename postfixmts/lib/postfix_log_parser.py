#
# Postfix log parser.
#
# I used pyparsing, Postfix log is too complex to use regex.
#
# The grammar needs a cleanup, but it works.
#
# Diego Woitasen - XTECH
# <diegows@xtech.com.ar>
#

from datetime import date, time, datetime
from pyparsing import *
import sys
import string
import re

class PostfixLogParser:
    # Log line fields constants definition
    MONTH = 'month'
    DAY = 'day'
    TIME = 'time'
    DATE = 'date'
    HOSTNAME = 'hostname'
    PROCESS = 'process'
    PID = 'pid'
    QUEUE_ID = 'queue_id'
    CLIENT_IP_ADDR = 'client_ip_addr'
    CLIENT_HOSTNAME = 'client_hostname'
    SASL_METHOD = 'sasl_method'
    USERNAME = 'username'
    MESSAGE_ID = 'message-id'
    SENDER = 'sender'
    SIZE = 'size'
    NRCPT = 'nrcpt'
    MSG_ID_LINE = 'msg_id_line'
    CLIENT_LINE = 'client_line'
    FROM_LINE = 'from_line'
    RECIPIENT = 'recipient'
    RELAY_HOSTNAME = 'relay_host'
    RELAY_IP = 'relay_ip'
    RELAY_PORT = 'relay_port'
    DELAY = 'delay'
    DSN = 'dst'
    STATUS = 'status'
    STATUS_MSG = 'status_msg'
    PORT = 'port'
    SENT_LINE = 'sent_line'
    ORIG_TO = 'orig_to'
    RELAY_TRANSPORT = 'transport'
    DELAYS = 'delays'
    CONN_USE = 'conn_use'
    HOST_FAIL_MSG = 'host_fail_msg'
    HOST_FAIL_LINE = 'host_fail_line'
    ERROR_CODE = 'error_code'
    ERROR_MSG = 'error_msg'
    PROTO = 'proto'
    REJECT_LINE = 'reject_line'
    MAIL_ADDR = 'mail_addr'

    integer = Word(nums)
    alnums = Word(alphanums)
    all_chars = Word(printables)
    xfloat = Word(nums + '.')

    month = Word(string.uppercase, string.lowercase, exact=3)(MONTH)

    day = integer(DAY)

    time = Combine(integer + ':' + integer + ':' + integer)(TIME)

    hostname = Word(alphanums + '.' + '-')

    process = Literal('postfix') + Suppress('/') + alnums(PROCESS)

    pid = Suppress("[") + integer + Suppress("]") + Suppress(":")
    pid = pid(PID)

    queue_id = Word(hexnums)(QUEUE_ID) + Suppress(":")

    ipAddr = delimitedList(integer, ".", combine=True)

    info = Suppress("<info>")

    client = Optional(Suppress('client=')) + hostname(CLIENT_HOSTNAME) + \
                Suppress("[") + ipAddr(CLIENT_IP_ADDR) + Suppress("]") + \
                Optional(Suppress(':'))

    auth = Suppress(',') + Suppress(SASL_METHOD) + Suppress('=') + \
                alnums("method") + Suppress(',') + Suppress('sasl_username') + \
                Suppress('=') + alnums(USERNAME)

    log_prefix = month + day + time + hostname(HOSTNAME) + info + process + pid

    message_id_chars = Word(string.letters + string.digits + \
                           string.punctuation)
    message_id = Suppress('message-id') + Suppress('=') + \
                    message_id_chars(MESSAGE_ID)

    local_part = Word(alphanums + '!#$%&\'*+-/=?^_`{|}~". ')
    mail_addr = Combine(Suppress('<') + local_part + Literal('@') + \
                    hostname + Suppress('>') + \
                    Optional((Suppress(',') | Suppress(':'))))

    sender = Suppress('from=') + mail_addr(SENDER)

    size = Suppress("size=") + integer(SIZE) + Suppress(",")

    nrcpt = Suppress("nrcpt=") + integer(NRCPT)

    recipient = Suppress('to=') + mail_addr(RECIPIENT)

    orig_to = Suppress('orig_to=') + mail_addr(ORIG_TO) 

    relay_host = hostname(RELAY_HOSTNAME) + Suppress("[") + ipAddr(RELAY_IP) + \
                Suppress("]") + Suppress(':') + integer(RELAY_PORT)

    relay_transport = Word(alphanums + '_-')(RELAY_TRANSPORT)

    relay = Suppress('relay=') + ( relay_host | relay_transport) + Suppress(',')

    delay = Suppress('delay=') + xfloat(DELAY) + Suppress(',')

    delays = Suppress('delays=') + xfloat + Suppress('/') + \
                xfloat + Suppress('/') + xfloat + Suppress('/') + xfloat + \
                Suppress(',')
    delays = Group(delays)(DELAYS)

    dsn = delimitedList(integer, ".", combine=True)

    dsn2 = dsn(DSN)

    dsn = Suppress('dsn=') + dsn(DSN) + Suppress(',')

    status = Suppress('status=') + alnums(STATUS)

    status_msg = Combine(Literal('(') + restOfLine)(STATUS_MSG)

    conn_use = Suppress('conn_use=') + integer(CONN_USE) + Suppress(',')

    error_code = integer(ERROR_CODE)

    error_msg = Word(printables.replace(';', '') + ' \t')(ERROR_MSG) + \
                    Suppress(';')

    proto = Suppress('proto=') + alnums(PROTO)

    host = ipAddr | hostname

    helo = Suppress('helo=<') + host + Suppress('>')

    client_line = log_prefix + queue_id + client + Optional(auth)

    msg_id_line = log_prefix + queue_id + message_id 

    from_line = log_prefix + queue_id + sender + size + nrcpt + \
                    Suppress("(queue active)")

    #sent_line: Mail sent or mail that postfix tried to send but fails for some
    #reason (4xx o 5xx)
    sent_line = log_prefix + queue_id + recipient + Optional(orig_to) + \
                    relay + Optional(conn_use) + delay + delays + dsn + \
                    status + status_msg

    host_fail_line = log_prefix + queue_id + restOfLine(HOST_FAIL_MSG)

    reject_line = log_prefix + Suppress('NOQUEUE: reject: RCPT from') + \
                    client + error_code + dsn2 + \
                    mail_addr(MAIL_ADDR) + error_msg + sender + recipient + \
                    proto + helo

    log_line = client_line(CLIENT_LINE) | msg_id_line(MSG_ID_LINE) | \
                from_line(FROM_LINE) | sent_line(SENT_LINE) | \
                host_fail_line(HOST_FAIL_LINE) | \
                reject_line(REJECT_LINE)

    def __init__(self):
        pass

    def fix_datetime(self, log_line):
        year = date.today().year
        date_str = '%s %s %s' % \
                    (log_line[self.DAY], log_line[self.MONTH], year)
        date_obj = datetime.strptime(date_str, '%d %b %Y').date()

        time_obj = datetime.strptime(log_line[self.TIME], '%H:%M:%S').\
                        time()

        log_line[self.DATE] = date_obj
        log_line[self.TIME] = time_obj

        return log_line

    def parse(self, line):
        try:
            log_line = self.log_line.parseString(line[:-1])
            log_line = self.fix_datetime(log_line)
            if self.CLIENT_LINE in log_line:
                self.process_client_line(log_line)
            if self.MSG_ID_LINE in log_line:
                self.process_msg_id_line(log_line)
            if self.FROM_LINE in log_line:
                self.process_from_line(log_line)
            if self.SENT_LINE in log_line:
                self.process_sent_line(log_line)
            if self.HOST_FAIL_LINE in log_line:
                self.process_host_fail_line(log_line)
            if self.REJECT_LINE in log_line:
                self.process_reject_line(log_line)
        except ParseException, e:
            self.process_unknown(log_line)
            return True

    def process_client_line(self, log_line):
        print log_line

    def process_msg_id_line(self, log_line):
        print log_line

    def process_from_line(self, log_line):
        print log_line

    def process_sent_line(self, log_line):
        print log_line

    def process_host_fail_line(self, log_line):
        print log_line

    def process_reject_line(self, log_line):
        print log_line

    def process_unknown(self, log_line):
        print 'UNKNOWN', log_line


if __name__ == '__main__':
    postfix_parser = PostfixLogParser()

    while True:
        line = sys.stdin.readline()
        if not line:
            break
        postfix_parser.parse(line)


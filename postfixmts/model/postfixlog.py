#
# Postfix Log Model
#

from sqlalchemy import ForeignKey, Column
from sqlalchemy.orm import relation, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import Date, Time, Integer, Unicode, Float
from sqlalchemy.orm.exc import NoResultFound

from datetime import date

from postfixmts.model import DeclarativeBase, metadata, DBSession

class ForeignValues():
    """
    Foreign key cache class. Store objects in a dict to avoid access to the
    database. If the object doesn't exists in the cache or in the database,
    it's created and stored in both places.

    Derived class must have the text attribute which is used as cache key.

    TODO: May be we should use memcached or something like that.
    """

    fk_cache = {}
    """ The cache """

    @classmethod
    def get(cls, text):
        """
        Get an object from the cache or the database using the key `text`.
        If the object doesn't exists, it'll be stores in the cache and in
        the database.
        """
        cls.check(text)

        if cls.fk_cache.has_key(text):
            return cls.fk_cache[text]

        query = DBSession.query(cls)
        query._autoflush = False
        try:
            data = query.filter(cls.text == text).one()
        except NoResultFound:
            data = cls()
            data.text = text
            DBSession.add(data)

        cls.fk_cache[text] = data

        return data

    @classmethod
    def check(cls, check):
        """
        Each class should implement this function to check if the text format
        is valid. Raise an exception on failure.
        """
        return True

    def __repr__(self):
        print self.text

    def __str__(self):
        print self.text


class SaslMethod(DeclarativeBase, ForeignValues):
    __tablename__ = 'sasl_method'

    id = Column(Integer, autoincrement=True, primary_key=True)

    text = Column(Unicode(20), nullable = False, unique = True)


class SaslUsername(DeclarativeBase, ForeignValues):
    __tablename__ = 'sasl_username'

    id = Column(Integer, autoincrement=True, primary_key=True)

    text = Column(Unicode(20), nullable = False, unique = True)


class MailAddress(DeclarativeBase, ForeignValues):
    __tablename__ = 'mail_address'

    id = Column(Integer, autoincrement=True, primary_key=True)

    text = Column(Unicode(120), nullable = False, unique = True)


class Status(DeclarativeBase, ForeignValues):
    __tablename__ = 'status'

    id = Column(Integer, autoincrement=True, primary_key=True)

    text = Column(Unicode(32), nullable = False, unique = True)


class ClientLine(DeclarativeBase):
    """
    Sep 27 09:58:05 smtpserver <info> postfix/smtpd[6900]: 8EE541568:
    client=unknown[10.3.4.26], sasl_method=PLAIN, sasl_username=pepe
    """
    __tablename__ = 'client_line'

    id = Column(Integer, autoincrement=True, primary_key=True)

    date = Column(Date, default=date.today)

    time = Column(Time, default = 0)

    hostname = Column(Unicode(120), nullable = False)

    queue_id = Column(Unicode(80), nullable = False)

    client_hostname = Column(Unicode(120), nullable = True)
    client_ip_addr = Column(Unicode(20), nullable = False)

    sasl_method_id = Column(Integer, ForeignKey('sasl_method.id'))
    sasl_method = relation('SaslMethod', backref = backref('sasl_method'))

    sasl_username_id = Column(Integer, ForeignKey('sasl_username.id'))
    sasl_username = relation('SaslUsername', backref = backref('sasl_username'))


class MsgIdLine(DeclarativeBase):
    """
    Sep 27 00:01:58 smtpserver <info> postfix/cleanup[8962]: 032BF146F:
    message-id=<246183886.QAIGEMNN7340BUON4898378@bla.bla.com>
    """
    __tablename__ = 'msg_id_line'

    id = Column(Integer, autoincrement=True, primary_key=True)

    date = Column(Date, default=date.today)

    time = Column(Time, default = 0)

    hostname = Column(Unicode(120), nullable = False)

    queue_id = Column(Unicode(80), nullable = False)

    msg_id = Column(Unicode(120), nullable = False)


class FromLine(DeclarativeBase):
    """
    Sep 27 00:30:23 smtpserver <info> postfix/qmgr[15879]: 46B7918E1:
    from=<ppepe@example.com>, size=16324, nrcpt=1 (queue active)
    """
    __tablename__ = 'from_line'

    id = Column(Integer, autoincrement=True, primary_key=True)

    date = Column(Date, default=date.today)

    time = Column(Time, default = 0)

    hostname = Column(Unicode(120), nullable = False)

    queue_id = Column(Unicode(80), nullable = False)

    sender_id = Column(Integer, ForeignKey('mail_address.id'))
    sender = relation('MailAddress', backref = backref('mail_address'))

    size = Column(Integer)

    nrcpt = Column(Integer)


class SentLine(DeclarativeBase):
    """
    Sep 27 00:00:48 smtpserver <info> postfix/lmtp[8963]: 3674F146F:
    to=<pepe@example.coop>, relay=imap.example.coop[10.6.2.40]:2003, delay=0.18,
    delays=0.11/0/0/0.06, dsn=2.1.5, status=sent (250 2.1.5 Ok)
    """
    __tablename__ = 'sent_line'

    id = Column(Integer, autoincrement=True, primary_key=True)

    date = Column(Date, default=date.today)

    time = Column(Time, default = 0)

    hostname = Column(Unicode(120), nullable = False)

    queue_id = Column(Unicode(80), nullable = False)

    recipient_join = 'SentLine.recipient_id == MailAddress.id'
    recipient_id = Column(Integer, ForeignKey('mail_address.id'))
    recipient = relation('MailAddress', primaryjoin = recipient_join)

    #Original recipient after alias processing.
    orig_to_join = 'SentLine.orig_to_id == MailAddress.id'
    orig_to_id = Column(Integer, ForeignKey('mail_address.id'))
    orig_to = relation('MailAddress', primaryjoin = orig_to_join)

    relay_hostname = Column(Unicode(120))
    relay_ip_port = Column(Unicode(32))
    relay_transport = Column(Unicode(32))
    
    delay = Column(Float)
    delays = Column(Unicode(32))

    dsn = Column(Unicode(6))
    
    status_id = Column(Integer, ForeignKey('status.id'))
    status = relation('Status', backref = backref('status'))

    status_msg = Column(Unicode(200))


class HostFailLine(DeclarativeBase):
    """
    Sep 30 23:31:14 smtpserver <info> postfix/smtp[30431]: 59E15400C7E7: host
    smtpxx.blabla.com[60.70.90.100] refused to talk to me: 421 4.7.0 [GL01]
    Message from (60.70.90.100) temporarily deferred - 4.16.50. 
    """
    __tablename__ = 'host_fail_line'

    id = Column(Integer, autoincrement=True, primary_key=True)

    date = Column(Date, default=date.today)

    time = Column(Time, default = 0)

    hostname = Column(Unicode(120), nullable = False)

    queue_id = Column(Unicode(80), nullable = False)

    msg = Column(Unicode(200))


class RejectLine(DeclarativeBase):
    """
    Sep 30 23:45:25 smtpserver <info> postfix/smtpd[353]: NOQUEUE: reject: RCPT
    from unknown[192.168.240.39]: 550 5.1.1 <pepe@example.gov.ar>: Recipient
    address rejected: User unknown in local recipient table;
    from=<somewhere@somesite.com> to=<pepe@example.gov.ar> proto=SMTP
    helo=<smtp.example.gov.ar>
    """
    __tablename__ = 'reject_line'

    id = Column(Integer, autoincrement=True, primary_key=True)

    date = Column(Date, default=date.today)

    time = Column(Time, default = 0)

    hostname = Column(Unicode(120), nullable = False)

    client_hostname = Column(Unicode(120))
    client_ip_addr = Column(Unicode(120))

    error_code = Column(Integer)

    dsn = Column(Unicode(6))

    mail_addr_join = 'RejectLine.mail_addr_id == MailAddress.id'
    mail_addr_id = Column(Integer, ForeignKey('mail_address.id'))
    mail_addr = relation('MailAddress', primaryjoin = mail_addr_join)

    error_msg = Column(Unicode(200))

    sender_join = 'RejectLine.sender_id == MailAddress.id'
    sender_id = Column(Integer, ForeignKey('mail_address.id'))
    sender = relation('MailAddress', primaryjoin = sender_join)

    recipient_join = 'RejectLine.recipient_id == MailAddress.id'
    recipient_id = Column(Integer, ForeignKey('mail_address.id'))
    recipient = relation('MailAddress', primaryjoin = recipient_join)


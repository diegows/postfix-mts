from sqlalchemy import ForeignKey, Column
from sqlalchemy.orm import relation, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import Date, Time, Integer, Unicode, Float

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from datetime import date

DeclarativeBase = declarative_base()

fk_cache = {}

uri = 'mysql://msgtrack:msgtrack@localhost/postfix_msgtrack'
engine = create_engine(uri)
DBSession = sessionmaker(bind = engine)()
metadata = DeclarativeBase.metadata
metadata.drop_all(engine)
metadata.create_all(engine)


class ForeignValues():
    @classmethod
    def get(cls, text):
        cls.check(text)

        if fk_cache.has_key(cls) and fk_cache[cls].has_key(text):
            return fk_cache[cls][text]

        query = DBSession.query(cls)
        query._autoflush = False
        data = query.filter(cls.text == text).all()
        if len(data) == 0:
            data = cls()
            data.text = text
            DBSession.add(data)
        elif len(data) == 1:
            data = data[0]
        else:
            #XXX: do this better
            raise Exception()

        if not fk_cache.has_key(cls):
            fk_cache[cls] = {}
        fk_cache[cls][text] = data
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
    __tablename__ = 'client_line'

    id = Column(Integer, autoincrement=True, primary_key=True)

    date = Column(Date, default=date.today)

    time = Column(Date, default = 0)

    hostname = Column(Unicode(120), nullable = False)

    queue_id = Column(Unicode(80), nullable = False)

    client_hostname = Column(Unicode(120), nullable = True)
    client_ip_addr = Column(Unicode(20), nullable = False)

    sasl_method_id = Column(Integer, ForeignKey('sasl_method.id'))
    sasl_method = relation('SaslMethod', backref = backref('sasl_method'))

    sasl_username_id = Column(Integer, ForeignKey('sasl_username.id'))
    sasl_username = relation('SaslUsername', backref = backref('sasl_username'))


class MsgIdLine(DeclarativeBase):
    __tablename__ = 'msg_id_line'

    id = Column(Integer, autoincrement=True, primary_key=True)

    date = Column(Date, default=date.today)

    time = Column(Date, default = 0)

    hostname = Column(Unicode(120), nullable = False)

    queue_id = Column(Unicode(80), nullable = False)

    msg_id = Column(Unicode(120), nullable = False)


class FromLine(DeclarativeBase):
    __tablename__ = 'from_line'

    id = Column(Integer, autoincrement=True, primary_key=True)

    date = Column(Date, default=date.today)

    time = Column(Date, default = 0)

    hostname = Column(Unicode(120), nullable = False)

    queue_id = Column(Unicode(80), nullable = False)

    sender_id = Column(Integer, ForeignKey('mail_address.id'))
    sender = relation('MailAddress', backref = backref('mail_address'))

    size = Column(Integer)

    nrcpt = Column(Integer)


class SentLine(DeclarativeBase):
    __tablename__ = 'sent_line'

    id = Column(Integer, autoincrement=True, primary_key=True)

    date = Column(Date, default=date.today)

    time = Column(Date, default = 0)

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
    __tablename__ = 'host_fail_line'

    id = Column(Integer, autoincrement=True, primary_key=True)

    date = Column(Date, default=date.today)

    time = Column(Date, default = 0)

    hostname = Column(Unicode(120), nullable = False)

    queue_id = Column(Unicode(80), nullable = False)

    msg = Column(Unicode(200))


class RejectLine(DeclarativeBase):
    __tablename__ = 'reject_line'

    id = Column(Integer, autoincrement=True, primary_key=True)

    date = Column(Date, default=date.today)

    time = Column(Date, default = 0)

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


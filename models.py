from sqlalchemy import Column, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import String, DateTime, Integer
from sqlalchemy.ext.declarative import declarative_base
import datetime
import settings

Base = declarative_base()
engine = create_engine(settings.DATABASE_URI)
Session = sessionmaker(bind=engine)


class LogEntryRaw(Base):
    __tablename__ = 'log_entry_raw'
    id = Column(Integer, primary_key=True)
    bucket_owner = Column(String(64))
    bucket = Column(String(63))
    time = Column(DateTime(timezone=True), index=True)
    remote_ip = Column(String(15))
    requester = Column(String(64))
    request_id = Column(String(16))
    operation = Column(String(50))
    key = Column(String(1024))
    request_uri = Column(String(1200))
    http_status = Column(String(3))
    error_code = Column(String(50))
    bytes_sent = Column(Integer)
    object_size = Column(Integer)
    total_time = Column(Integer)
    turnaround_time = Column(Integer)
    referrer = Column(String(2083))
    user_agent = Column(String(65536))
    version_id = Column(String(100))

    def __init__(self,
                 bucket_owner,
                 bucket,
                 time,
                 remote_ip,
                 requester,
                 request_id,
                 operation,
                 key,
                 request_uri,
                 http_status,
                 error_code,
                 bytes_sent,
                 object_size,
                 total_time,
                 turnaround_time,
                 referrer,
                 user_agent,
                 version_id):
        self.bucket_owner = bucket_owner
        self.bucket = bucket
        self.time = _time(time)
        self.remote_ip = remote_ip
        self.requester = requester
        self.request_id = request_id
        self.operation = operation
        self.key = key
        self.request_uri = request_uri
        self.http_status = http_status
        self.error_code = error_code
        self.bytes_sent = _int(bytes_sent)
        self.object_size = _int(object_size)
        self.total_time = _int(total_time)
        self.turnaround_time = _int(turnaround_time)
        self.referrer = referrer
        self.user_agent = user_agent
        self.version_id = version_id


def _time(s):
    return datetime.datetime.strptime(s, '[%d/%b/%Y:%H:%M:%S %z]')


def _int(s):
    return 0 if s == '-' else int(s)


def create_tables():
    Base.metadata.create_all(engine)

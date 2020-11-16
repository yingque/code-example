"""
python 3.7.6
sqlalchemy==1.3.20

doc ref: https://docs.sqlalchemy.org/en/13/core/event.html
"""
from datetime import datetime

from sqlalchemy import (
    Column,
    create_engine,
    DateTime,
    event,
    inspect,
    Integer,
    String,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (
    Session as BaseSession,
    sessionmaker,
)

db_uri = "sqlite:///:memory:"
engine_options = {
    "pool_pre_ping": True
}
session_options = {
    "expire_on_commit": False,
    "autocommit": False,
    "autoflush": False
}


class Session(BaseSession):

    def __init__(self, *args, **kwargs):
        self.commit_on_exit = kwargs.pop("commit_on_exit", True)
        super(Session, self).__init__(*args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.commit_on_exit:
                self.commit()
        except Exception:
            self.rollback()
            raise
        finally:
            self.close()


class SqlAlchemy(object):

    def __init__(self):
        self.Model = declarative_base()

        self._engine = None
        self._session_factory = None

    @property
    def engine(self):
        if not self._engine:
            self._engine = create_engine(db_uri, **engine_options)
        return self._engine

    def get_session(self, **kwargs):
        if not self._session_factory:
            self._session_factory = sessionmaker(
                bind=self.engine,
                class_=Session,
                **session_options
            )
        return self._session_factory(**kwargs)


db = SqlAlchemy()


class User(db.Model):
    """user model"""
    __tablename__ = "t_user"

    pid = Column("id", Integer, primary_key=True, autoincrement=True)
    name = Column("name", String(32), unique=True, nullable=False)
    create_time = Column("create_time", DateTime, nullable=False, default=datetime.now)
    update_time = Column("update_time", DateTime, nullable=False, default=datetime.now,
                         onupdate=datetime.now)
    remark = Column("remark", String(128), nullable=True)


# create all table
db.Model.metadata.bind = db.engine
db.Model.metadata.create_all()


@event.listens_for(Session, "after_begin")
def receive_after_begin(session, transaction, connection):
    print("recevie session begin event")


@event.listens_for(Session, "after_rollback")
def receive_after_rollback(session):
    print("recevie session rollback event")


@event.listens_for(Session, "after_commit")
def receive_after_commit(session):
    print("recevie session commit event")


def receive_after_insert(mapper, connection, model_ins):
    ins_state = inspect(model_ins)
    print("recevie ins insert event")
    print("this is model ins:", model_ins)
    print("this is ins state:", ins_state)
    print("this is ins session:", ins_state.session)


def receive_after_update(mapper, connection, model_ins):
    ins_state = inspect(model_ins)
    print("recevie ins update event")
    print("this is model ins:", model_ins)
    print("this is ins state:", ins_state)
    print("this is ins session:", ins_state.session)


def receive_after_delete(mapper, connection, model_ins):
    ins_state = inspect(model_ins)
    print("recevie ins delete event")
    print("this is model ins:", model_ins)
    print("this is ins state:", ins_state)
    print("this is ins session:", ins_state.session)


def base_event_register(model: db.Model):
    event.listen(model, "after_insert", receive_after_insert)
    event.listen(model, "after_update", receive_after_update)
    event.listen(model, "after_delete", receive_after_delete)


# user model regist event    
base_event_register(User)

if __name__ == "__main__":
    user_1 = User(name="aaa")

    with db.get_session() as session:
        print("insert", "***" * 20)
        session.add(user_1)

    with db.get_session() as session:
        print("update", "***" * 20)
        user_1.name = "bbb"
        session.add(user_1)

    with db.get_session() as session:
        print("delete", "***" * 20)
        session.delete(user_1)

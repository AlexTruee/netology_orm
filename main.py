import os

import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker
from sqlalchemy import or_
from models import create_tables, fill_tables, Publisher, Book, Sale, Shop, Stock
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
LOGIN = os.environ.get('LOGIN')
PSW = os.environ.get('PSW')
HOST = os.environ.get('HOST')
PORT = os.environ.get('PORT')
NAME_BD = os.environ.get('NAME_BD')

DSN = f'postgresql://{LOGIN}:{PSW}@{HOST}:{PORT}/{NAME_BD}'

engine = sq.create_engine(DSN)

create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

# Наполняем таблицы
fill_tables(session, 'test_data.json')


def query_result(name=None, id_=None):
    results = session.query(Publisher.name, Book.title, Shop.name, Sale.price, Sale.date_sale). \
        join(Book).join(Stock).join(Shop).join(Sale). \
        filter(or_(Publisher.id == id_, Publisher.name == name)).all()

    for result in results:
        print("{: <40} | {: <10} | {:4.2f} | {: <}".format(*result[1:-1], result[-1].strftime("%d-%m-%Y")))


publisher = input('Введите имя или идентификатор издателя\n')

try:
    if isinstance(int(publisher), int):
        query_result(id_=publisher)
except ValueError:
    query_result(name=publisher)
finally:
    session.close()
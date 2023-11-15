import params as pa
import pandas as pd

import psycopg2
from sqlalchemy import create_engine

def DataReader(Schema, Table, Col, Value):
    query=(""" SELECT * from "%s"."%s" where "%s" = '%s' """
           % (Schema, Table, Col, Value))
    conn_string = ('postgresql://' + pa.user + ":"
                   + pa.password + "@"
                   + pa.host + ":"
                   + str(pa.port) + "/" + pa.dbname)
    alchemyEngine = create_engine(conn_string)
    dbConnection = alchemyEngine.connect()
    Data = pd.read_sql(query, con=dbConnection)
    dbConnection.close()
    alchemyEngine.dispose()

    return Data

def DataInsert(Schema, Table, Col1, Value1,Col2,Value2):
    query=(""" INSERT INTO "%s"."%s" ("%s", "%s") values ('%s', '%s') """
           % (Schema, Table, Col1,Col2, Value1,Value2))
    conn = psycopg2.connect(host=pa.host,
                            dbname=pa.dbname,
                            user=pa.user,
                            password=pa.password,
                            port=pa.port)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

    return []

if __name__ == '__main__':
    Schema='public'
    Table="student"
    Col='name'
    Value="Jung"
    Data= DataReader(Schema, Table, Col, Value)


    print(Data)

    DataInsert(Schema, Table, 'name', 'Yoon',
               'studentnumber', 555444999)

    print(91)
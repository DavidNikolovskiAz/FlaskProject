import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import DeclarativeBase,Mapped,mapped_column
from sqlalchemy import MetaData,func
from flask import Flask,request,render_template
import jsonify
import json

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///E:/PythonProjects/FlProject/users_vouchers.db'
class Base(DeclarativeBase):
    pass
engine = sqlalchemy.create_engine('sqlite:///users_vouchers.db',echo=True,connect_args={"timeout": 30})
connection=engine.connect()
Session = sessionmaker(bind=engine)
session = Session()
metadata=MetaData()

class high_spenders(Base):
    __tablename__="high_spenders"
    user_id:Mapped[int]=mapped_column(primary_key=True)
    total_spending:Mapped[int]

user_info=sqlalchemy.Table(
    "user_info",
    metadata,
    sqlalchemy.Column("user_id",sqlalchemy.Integer),
    sqlalchemy.Column("name",sqlalchemy.String),
    sqlalchemy.Column("email",sqlalchemy.String),
    sqlalchemy.Column("age",sqlalchemy.Integer)
)
user_spending=sqlalchemy.Table(
    "user_spending",
    metadata,
    sqlalchemy.Column("user_id",sqlalchemy.Integer),
    sqlalchemy.Column("money_spent",sqlalchemy.Integer),
    sqlalchemy.Column("year",sqlalchemy.Integer)
)

def insert_user(user_id:int,total_spending:int):
    user=high_spenders(user_id=user_id,total_spending=total_spending)
    session.add(user)
    session.commit()

def select_user(user_id:int):
        total_spent=session.query(
            user_spending.c.user_id,
            func.sum(user_spending.c.money_spent).label('total')
        ).filter(user_spending.c.user_id==user_id)
        json_data=[u._asdict() for u in total_spent]
        json_output=json.dumps(json_data)
        return json_output

def avg_age():
    avg18_24=session.query(
        func.avg(user_spending.c.money_spent).label('18-24')
    ).join(user_info,user_info.c.user_id==user_spending.c.user_id).filter(user_info.c.age > 17).filter(user_info.c.age <25)
    json_data=[u._asdict() for u in avg18_24]
    avg25_30=session.query(
        func.avg(user_spending.c.money_spent).label('25-30')
    ).join(user_info,user_info.c.user_id==user_spending.c.user_id).filter(user_info.c.age > 24).filter(user_info.c.age <31)
    json_data1=[u._asdict() for u in avg25_30]
    avg31_36 = session.query(
        func.avg(user_spending.c.money_spent).label('31-36')
    ).join(user_info, user_info.c.user_id == user_spending.c.user_id).filter(user_info.c.age > 30).filter(user_info.c.age < 37)
    json_data2 = [u._asdict() for u in avg31_36]
    avg37_47 = session.query(
        func.avg(user_spending.c.money_spent).label('37-47')
    ).join(user_info, user_info.c.user_id == user_spending.c.user_id).filter(user_info.c.age > 36).filter(user_info.c.age < 48)
    json_data3 = [u._asdict() for u in avg37_47]
    avg47 = session.query(
        func.avg(user_spending.c.money_spent).label('47>')
    ).join(user_info, user_info.c.user_id == user_spending.c.user_id).filter(user_info.c.age > 47)
    json_data4 = [u._asdict() for u in avg47]
    json_output=json.dumps(json_data+json_data1+json_data2+json_data3+json_data4)
    return json_output

@app.route("/")
def index():
    return "<h1>Home</h1>"

@app.route("/total_spent/<int:user_id>", methods=["GET"])
def total(user_id):
    return select_user(user_id)

@app.route("/average_spending_by_age", methods=["GET"])
def avgspent():
    return avg_age()

@app.route("/write_high_spending_user", methods=["POST", "GET"])
def spenders():
    if request.method=="POST":
        user_id=request.form["user_id"]
        total_spending=request.form["total_spending"]
        return insert_user(int(user_id), int(total_spending))
    else:
        return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)
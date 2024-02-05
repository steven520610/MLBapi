# Modified at: 2024.2.5
# Author: Steven
# Usage: operation related to db
import json
import time
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Integer,
    DateTime,
    inspect,
    select,
    update,
    insert,
)
from api import team_list
from config import db_URI

db = MetaData()
engine = create_engine(db_URI)
update_cnt, insert_cnt = 0, 0


def updatePlayers(team):
    global update_cnt, insert_cnt
    # Load JSON file
    f = open(f"MLB_player/{team}.json")
    players = json.load(f)

    # Check whether table exists or not
    inspector = inspect(engine)
    if "Players" not in inspector.get_table_names():
        addTable()

    # Load table & read columns from table
    table = Table("Players", db, autoload_with=engine)
    columns = inspector.get_columns("Players")
    columns_name = []
    for column in columns:
        columns_name.append(column["name"])

    # Select rows in the table first
    # and decide whether we need to update or insert
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            query = select(table.c.PlayerID)
            cursor = connection.execute(query)
            results = cursor.fetchall()
            selectID = []
            for row in results:
                selectID.append(row[0])
            transaction.commit()
        except:
            transaction.rollback()
            raise RuntimeError

    insert_data = []
    # Iterate each player in the dict from JSON file
    for player in players:
        data = {}
        # Because I don't choose every column from JSON,
        # I use some condition to solve this problem.
        for key, value in player.items():
            # DateTime column
            if key in ["BirthDate", "ProDebut", "InjuryStartDate"]:
                if value == None:
                    data[key] = None
                else:
                    data[key] = value.replace("T", " ")
            # String, Integer column
            elif key in columns_name:
                if value == None:
                    data[key] = None
                else:
                    data[key] = value
        # Decide to use insert or update
        # update
        if data["PlayerID"] in selectID:
            # Because I use single comparison based on PlayerID,
            # it may cause some efficiency problem
            # by connecting to db for every player
            with engine.connect() as connection:
                transaction = connection.begin()
                try:
                    statement = (
                        update(table)
                        .where(table.c.PlayerID == data["PlayerID"])
                        .values(data)
                    )
                    connection.execute(statement)
                    transaction.commit()
                    update_cnt += 1
                except:
                    transaction.rollback()
                    raise RuntimeError
        # insert
        else:
            insert_cnt += 1
            insert_data.append(data)

    if insert_data:
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                statement = insert(table).values(insert_data)
                connection.execute(statement)
                transaction.commit()
            except:
                transaction.rollback()
                raise RuntimeError
    f.close()


def addTable():
    columns = [
        Column("PlayerID", Integer, primary_key=True),
        Column("Status", String(50)),
        Column("TeamID", Integer),
        Column("Team", String(3)),
        Column("Jersey", Integer),
        Column("PositionCategory", String(4)),
        Column("Position", String(4)),
        Column("FirstName", String(30)),
        Column("LastName", String(30)),
        Column("BatHand", String(3)),
        Column("ThrowHand", String(3)),
        Column("Height", Integer),
        Column("Weight", Integer),
        # 要調成DateTime的話，需要對json檔內的字串做處理：中間有一個T(ISO格式)
        Column("BirthDate", DateTime),
        Column("ProDebut", DateTime),
        Column("InjuryStatus", String(30)),
        Column("InjuryBodyPart", String(30)),
        Column("InjuryStartDate", DateTime),
        Column("Experience", Integer),
    ]
    table = Table("Players", db, *columns)
    db.create_all(engine, [table])


def main():
    for team in team_list:
        updatePlayers(team)
        print(f"{team} update success.")
    print(f"Update {update_cnt} player(s).")
    print(f"Insert {insert_cnt} player(s).")


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Total Runtime is: {end_time-start_time}s.")

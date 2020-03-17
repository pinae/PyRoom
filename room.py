from datetime import date, datetime
from typing import cast, Sequence
import sqlite3
import inspect


class Database:
    def __init__(self, filename: str = ":memory:"):
        sqlite3.register_adapter(bool, lambda x: 1 if x else 0)
        sqlite3.register_converter("bool", lambda x: True if x > 0 else False)
        self.conn = sqlite3.connect(filename)

    def __del__(self):
        self.conn.close()


class Dao:
    def __init__(self, room_db: Database):
        self.db = room_db

    def check_table_exists(self, table_name: str):
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{0}'".format(table_name))
        return cursor.fetchone()[0] > 0


class KeyAnnotationClass:
    def __init__(self, key_type):
        self.t = key_type

    def get_type(self):
        return self.t


class PrimaryKey(KeyAnnotationClass):
    def __init__(self, key_type):
        if key_type not in [int, str, bytes]:
            raise SyntaxError(str(key_type) + " is an invalid primary key. Use int, str or bytes.")
        super().__init__(key_type)

    def sql_type(self) -> str:
        if self.t is int:
            return 'INT'
        elif self.t is str:
            return 'TEXT'
        elif self.t is bytes:
            return 'BLOB'


class Entity:
    def __init__(self, *args, **kwargs):
        self.primary_key()
        self._property_values = dict()
        arg_index = 0
        for col, col_type in self.__cols__():
            if col in kwargs.keys() and (type(kwargs[col]) is col_type or
                                         (issubclass(type(col_type), KeyAnnotationClass) and
                                          type(kwargs[col]) is col_type.get_type())):
                self._property_values[col] = kwargs[col]
            elif len(args) > arg_index and (type(args[arg_index]) is col_type or
                                            (issubclass(type(col_type), KeyAnnotationClass) and
                                             type(args[arg_index]) is col_type.get_type())):
                self._property_values[col] = args[arg_index]
                arg_index += 1
            else:
                raise SyntaxError(col + " is not in **kwargs or *args or has the wrong type." +
                                  " This is expected: " + str(self.__cols__()))

    def __str__(self):
        return "<{class_name}: {properties}>".format(
            class_name=self.__class__.__name__,
            properties=", ".join([
                prop + "=" + (str(self._property_values[prop])
                              if type(self._property_values[prop]) in [int, float, bool] else
                              '"{}"'.format(str(self._property_values[prop])))
                for prop, _ in self.__cols__()
            ]))

    @staticmethod
    def property_types():
        return [int, float, str, bytes, bool, date, datetime]

    @staticmethod
    def __type_to_sql_type__(t: type) -> str:
        if t is int:
            return 'INT'
        elif t is float:
            return 'REAL'
        elif t is str:
            return 'TEXT'
        elif t is bytes:
            return 'BLOB'
        elif t is None:
            return 'NULL'
        elif type(t) is PrimaryKey:
            return cast(PrimaryKey, t).sql_type()
        else:
            return t.__name__

    @classmethod
    def __cols__(cls):
        non_internal = [(prop, inspect.getattr_static(cls, prop)) for prop in dir(cls) if not prop.startswith('_')]
        return [(prop, t) for prop, t in non_internal if
                t in cls.property_types() or issubclass(type(t), KeyAnnotationClass)]

    def values(self) -> tuple:
        return tuple([self._property_values[prop] for prop in self._property_values.keys()])

    def primary_key(self):
        primary_key = None
        for col, col_type in self.__cols__():
            if type(col_type) is PrimaryKey:
                if primary_key is not None:
                    raise SyntaxError("Found more than one PrimaryKey()! There has to be exactly one.")
                primary_key = col
        if primary_key is not None:
            return primary_key
        else:
            raise SyntaxError("No PrimaryKey() found! You have to define exactly one.")

    @classmethod
    def __create_table_sql__(cls) -> str:
        return "CREATE TABLE IF NOT EXISTS `{0}`({1})".format(
            cls.__name__,
            ", ".join([
                "{0} {1}".format(col, cls.__type_to_sql_type__(t)) for col, t in cls.__cols__()
            ]))

    @classmethod
    def __insert_sql__(cls) -> str:
        return "INSERT INTO {0} VALUES ({1})".format(
            cls.__name__,
            ", ".join(list("?" * len(cls.__cols__()))))

    def update_sql(self) -> str:
        return "UPDATE {0} SET {1} WHERE {2} = {3}".format(
            self.__class__.__name__,
            ", ".join([col + " = ?" for col, _ in self.__cols__()]),
            self.primary_key(),
            self._property_values[self.primary_key()]
        )

    @classmethod
    def create_table(cls, dao: Dao):
        cursor = dao.db.conn.cursor()
        cursor.execute(cls.__create_table_sql__())
        dao.db.conn.commit()

    @classmethod
    def query(cls, sql: str) -> callable:
        def decorator(func: callable) -> callable:
            def wrapper(*args, **kwargs):
                param_pos = 0
                for param in inspect.signature(func).parameters:
                    if param not in kwargs:
                        kwargs[param] = args[param_pos]
                        param_pos += 1
                dao = kwargs['self']
                cls.create_table(dao)
                cursor = dao.db.conn.cursor()
                value_dict = kwargs.copy()
                value_dict.pop('self')
                cursor.execute(sql.format(cls.__name__, table=cls.__name__, **kwargs))
                return cursor.fetchall()
            return wrapper
        return decorator

    @classmethod
    def insert(cls) -> callable:
        def decorator(func: callable) -> callable:
            def wrapper(*args: cls):
                assert issubclass(type(args[0]), Dao)
                dao = cast(Dao, args[0])
                cls.create_table(dao)
                objects = cast(Sequence[cls], args[1:])
                cursor = dao.db.conn.cursor()
                cursor.executemany(cls.__insert_sql__(), [o.values() for o in objects])
                dao.db.conn.commit()
            return wrapper
        return decorator


class Food(Entity):
    uid = PrimaryKey(int)
    name = str
    calories = float
    last_log = datetime
    importance = float


class FoodDao(Dao):
    @Food.query("SELECT * FROM {table}")
    def allFood(self) -> list:
        pass

    @Food.query("SELECT * FROM {table} WHERE name LIKE '%{query}%'")
    def searchFood(self, query: str) -> list:
        pass

    @Food.insert()
    def insertFood(self, *food: Food):
        pass


class MyRoomDatabase(Database):
    def food_dao(self):
        return FoodDao(self)


if __name__ == "__main__":
    db = MyRoomDatabase()
    food_dao = db.food_dao()
    print(food_dao.searchFood("Kuch"))
    print(food_dao.allFood())
    print("---------------")
    foo = Food(datetime.now(), "Fu-Kuchen", 53, calories=123., importance=3.2)
    print(foo)
    food_dao.insertFood(foo)
    print(food_dao.searchFood("Kuch"))
    print(food_dao.allFood())
    print("---------------")
    print(foo.__cols__())
    c = db.conn.cursor()
    print("…sql…", foo.__create_table_sql__())
    c.execute(foo.__create_table_sql__())
    db.conn.commit()
    print(c.execute("PRAGMA table_info(`Food`);").fetchall())
    print("…sql…", foo.__insert_sql__())
    print("…sql…values…", foo.values())
    c.execute(foo.__insert_sql__(), foo.values())
    db.conn.commit()
    import time
    time.sleep(0.5)
    qc = db.conn.cursor()
    qc.execute(
        "UPDATE `Food` SET importance = (importance - (julianday(?) - julianday(last_log)) * importance)",
        (datetime.now(),))
    db.conn.commit()
    print(qc.execute("SELECT * FROM `Food`").fetchall())
    bar = Food(uid=1, name="Bar Bean", calories=612., last_log=datetime.now(), importance=1.)
    c.execute(bar.__insert_sql__(), bar.values())
    qc.execute("UPDATE `Food` SET calories = ? WHERE uid = 53", (20.315,))
    qc.execute("UPDATE `Food` SET calories = ? WHERE uid = 2", (7.2,))
    print(foo.update_sql(), foo.values())
    db.conn.commit()
    print(qc.execute("SELECT * FROM `Food`").fetchall())

from datetime import date, datetime
import sqlite3


class Database:
    def __init__(self, filename: str = ":memory:"):
        sqlite3.register_adapter(bool, lambda x: 1 if x else 0)
        sqlite3.register_converter("bool", lambda x: True if x > 0 else False)
        self.conn = sqlite3.connect(filename)

    def __del__(self):
        self.conn.close()


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

    def __cols__(self) -> list:
        return [(prop, self.__getattribute__(prop)) for prop in dir(self)
                if not prop.startswith("_") and (
                        self.__getattribute__(prop) in [int, float, str, bytes, bool, date, datetime] or
                        issubclass(type(self.__getattribute__(prop)), KeyAnnotationClass))]

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
            return t.sql_type()
        else:
            return t.__name__

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

    def create_table_sql(self) -> str:
        return "CREATE TABLE `{0}`({1})".format(
            self.__class__.__name__,
            ", ".join([
                "{0} {1}".format(col, self.__type_to_sql_type__(t)) for col, t in self.__cols__()
            ]))

    def insert_sql(self) -> str:
        return "INSERT INTO {0} VALUES ({1})".format(
            self.__class__.__name__,
            ", ".join(list("?"*len(self.__cols__()))))

    def update_sql(self) -> str:
        return "UPDATE {0} SET {1} WHERE {2} = {3}".format(
            self.__class__.__name__,
            ", ".join([col + " = ?" for col, _ in self.__cols__()]),
            self.primary_key(),
            self._property_values[self.primary_key()]
        )


class Food(Entity):
    uid = PrimaryKey(int)
    name = str
    calories = float
    last_log = datetime
    importance = float


if __name__ == "__main__":
    db = Database()
    foo = Food(datetime.now(), "Fu-Kuchen", 53, calories=123., importance=3.2)
    print(dir(foo))
    print(foo.__cols__())
    c = db.conn.cursor()
    print("…sql…", foo.create_table_sql())
    c.execute(foo.create_table_sql())
    db.conn.commit()
    print(c.execute("PRAGMA table_info(`Food`);").fetchall())
    print("…sql…", foo.insert_sql())
    print("…sql…values…", foo.values())
    c.execute(foo.insert_sql(), foo.values())
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
    c.execute(bar.insert_sql(), bar.values())
    qc.execute("UPDATE `Food` SET calories = ? WHERE uid = 53", (20.315,))
    qc.execute("UPDATE `Food` SET calories = ? WHERE uid = 2", (7.2,))
    print(foo.update_sql(), foo.values())
    db.conn.commit()
    print(qc.execute("SELECT * FROM `Food`").fetchall())

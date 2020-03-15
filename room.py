from datetime import date, datetime
import sqlite3


class Database:
    def __init__(self, filename: str = ":memory:"):
        sqlite3.register_adapter(bool, lambda x: 1 if x else 0)
        sqlite3.register_converter("bool", lambda x: True if x > 0 else False)
        self.conn = sqlite3.connect(filename)

    def __del__(self):
        self.conn.close()


class Entity:
    def __init__(self, *args, **kwargs):
        self.property_values = dict()
        arg_index = 0
        for col, col_type in self.__cols__():
            if col in kwargs.keys() and type(kwargs[col]) is col_type:
                self.property_values[col] = kwargs[col]
            elif len(args) > arg_index and type(args[arg_index]) is col_type:
                self.property_values[col] = args[arg_index]
                arg_index += 1
            else:
                raise SyntaxError(col + " is not in **kwargs or *args or has the wrong type." +
                                  " This is expected: " + str(self.__cols__()))

    def __cols__(self) -> list:
        return [(prop, self.__getattribute__(prop)) for prop in dir(self)
                if not prop.startswith("_") and self.__getattribute__(prop) in [
                    int, float, str, bytes, bool, date, datetime]]

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
        else:
            return t.__name__

    def values(self) -> tuple:
        return tuple([self.property_values[prop] for prop in self.property_values.keys()])

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


class Food(Entity):
    uid = int
    name = str
    calories = float
    last_log = datetime


if __name__ == "__main__":
    db = Database()
    foo = Food(datetime.now(), "Fu-Kuchen", 53, calories=123.)
    c = db.conn.cursor()
    print("…sql…", foo.create_table_sql())
    c.execute(foo.create_table_sql())
    db.conn.commit()
    print(c.execute("PRAGMA table_info(`Food`);").fetchall())
    print("…sql…", foo.insert_sql())
    print("…sql…values…", foo.values())
    c.execute(foo.insert_sql(), foo.values())
    db.conn.commit()
    qc = db.conn.cursor()
    print(qc.execute("SELECT * FROM `Food`").fetchall())

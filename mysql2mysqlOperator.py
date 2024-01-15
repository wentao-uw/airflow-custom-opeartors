from airflow.models.baseoperator import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

class MySqlToMySqlOperator(BaseOperator):
    """
    Custom operator to synchronize data from one MySQL database to another.
    """

    template_fields = ("source_mysql_table","target_mysql_table")

    def __init__(
        self,
        source_mysql_table,
        target_mysql_table,
        source_conn_id='source_mysql_conn',
        target_conn_id='target_mysql_conn',
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.source_mysql_table = source_mysql_table
        self.target_mysql_table = target_mysql_table
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id

    def execute(self, context):
        # Extract data from the source MySQL database
        source_hook = MySqlHook(mysql_conn_id=self.source_conn_id)
        source_connection = source_hook.get_conn()
        # 在airflow connection 中设置sscursor
        source_db_cursor = source_connection.cursor()
        target_hook = MySqlHook(mysql_conn_id=self.target_conn_id)
        target_connection = target_hook.get_conn()
        target_db_cursor = target_connection.cursor()
        try:
            source_db_cursor.execute(f"SELECT * FROM {self.source_mysql_table}")
            print("execute select query")
            # 获取列数
            num_columns = len(source_db_cursor.description)
            print(f"num_columns: {num_columns}")
            # 构建插入语句
            insert_sql = f"INSERT INTO {self.source_mysql_table} VALUES (" + ', '.join(['%s'] * num_columns) + ")"
            acc = 0
            batch_data = []
            # 将数据插入到目标数据库
            for row in source_db_cursor:
                batch_data.append(row)
                acc = acc + 1
                if acc % 1000 == 0:
                    print(f"insert {acc} rows")
                    target_db_cursor.executemany(insert_sql, batch_data)
                    batch_data = []
            if batch_data:
                target_db_cursor.executemany(insert_sql, batch_data)
            target_connection.commit()
        finally:
            # 关闭数据库连接
            source_db_cursor.close()
            target_db_cursor.close()
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
     
        source_hook = MySqlHook(mysql_conn_id=self.source_conn_id)
        source_connection = source_hook.get_conn()
        # set sscursor in airflow connection (from ui)
        source_db_cursor = source_connection.cursor()
        target_hook = MySqlHook(mysql_conn_id=self.target_conn_id)
        target_connection = target_hook.get_conn()
        target_db_cursor = target_connection.cursor()
        try:
            source_db_cursor.execute(f"SELECT * FROM {self.source_mysql_table}")

            # get the number of columns in the result set
            num_columns = len(source_db_cursor.description)

            # create an INSERT INTO sql string for the target table
            insert_sql = f"INSERT INTO {self.source_mysql_table} VALUES (" + ', '.join(['%s'] * num_columns) + ")"

            # execute the sql statement
            for row in source_db_cursor:
                target_db_cursor.execute(insert_sql, row)
            target_connection.commit()
        finally:
            # close connections
            source_db_cursor.close()
            target_db_cursor.close()

using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Data;
using Microsoft.SqlServer.Server;

namespace tSQLtCLR
{
    class TestDatabaseFacade : IDisposable
    {
        private SqlConnection connection;
        private SqlString infoMessage;
        Boolean disposed = false;

        public TestDatabaseFacade()
        {
            connect();
        }

        public void Dispose()
        {
            if (!disposed)
            {
                disconnect();
                disposed = true;
            }
            GC.SuppressFinalize(this);
        }

        public SqlString InfoMessage
        {
            get { return infoMessage; }
        }

        private void connect()
        {
            connection = new SqlConnection();
            connection.ConnectionString = "Context Connection=true;";
            connection.Open();
        }

        private void disconnect()
        {
            connection.Dispose();
        }

        public String ServerName
        {
            get
            {
                SqlDataReader reader = executeCommand("SELECT SERVERPROPERTY('ServerName');");
                reader.Read();
                String serverName = reader.GetString(0);
                reader.Close();
                return serverName;
            }
        }

        public String DatabaseName
        {
            get { return connection.Database; }
        }

        public SqlDataReader executeCommand(SqlString Command)
        {
            infoMessage = SqlString.Null;
            connection.InfoMessage += OnInfoMessage;
            SqlCommand cmd = new SqlCommand();

            cmd.Connection = connection;
            cmd.CommandText = Command.ToString();

            SqlDataReader dataReader = cmd.ExecuteReader(CommandBehavior.KeyInfo);

            return dataReader;
        }

        public List<List<string>> getDataTableColumns(string[] tablenames)
        {

            infoMessage = SqlString.Null;
            connection.InfoMessage += OnInfoMessage;
            SqlCommand cmd = new SqlCommand();

            cmd.Connection = connection;
            List<List<string>> colsCollection = new List<List<string>>();
            foreach (var tablename in tablenames)
            {
                if (!string.IsNullOrEmpty(tablename))
                {
                    using (var tbl = GetDataTable(cmd, tablename))
                    {
                        var columns = new List<string>();
                        colsCollection.Add(columns);
                        for (var i = 0; i < tbl.Columns.Count; i++)
                        {
                            columns.Add(tbl.Columns[i].ColumnName);
                        }
                    }
                }
                else
                {
                    colsCollection.Add(new List<string>());
                }
            }
            return colsCollection;
        }

        /*
         * Unfortunately, Microsoft in their infinite wisdom doesn't allow
         * bulk insert with context connections, bleh
         */
        //public void BulkCopyToSql(DataTable dtToSave)
        //{
        //    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
        //    {
        //        foreach (DataColumn c in dtToSave.Columns)
        //            bulkCopy.ColumnMappings.Add(c.ColumnName, c.ColumnName);

        //        bulkCopy.DestinationTableName = dtToSave.TableName;
        //        bulkCopy.WriteToServer(dtToSave);
        //    }
        //}

        public void InsertTableData(DataTable dtToSave)
        {
            var success = false;
            var insertStr = "INSERT INTO " + dtToSave.TableName + "(";
            var values = ") Values(";
            //add the column names
            foreach (DataColumn dcol in dtToSave.Columns)
            {
                insertStr = insertStr + "[" + dcol.ColumnName.ToString() + "], ";
                values = values + "@" + dcol.ColumnName.ToString().Replace(" ", string.Empty) + ", ";
            }

            //remove the last comma + form the final string
            insertStr = insertStr.Substring(0, insertStr.Length - 2);
            values = values.Substring(0, values.Length - 2);

            //build string that will look like this
            //inssert into <table> ([<col1>], [<col2>], ...) values (@<col1>, @<col2>, ....)
            //todo: this might be faster to build a command with a bunch of insert statements and the values put right into the string
            //but that would require type checking on the columns and conditionally quoting and making sure date formats, etc are correct.
            //it may not be worth the effort.
            insertStr = insertStr + values + ")";
            var cmd = new SqlCommand();

            foreach (DataRow drow in dtToSave.Rows)
            {
                cmd = new SqlCommand(insertStr, connection);
                foreach (DataColumn dcol in dtToSave.Columns)
                {
                    cmd.Parameters.AddWithValue("@" + dcol.ColumnName.ToString().Replace(" ", string.Empty), drow[dcol.ColumnName.ToString()]);
                }
                SqlContext.Pipe.ExecuteAndSend(cmd);
            }
        }

        private DataTable GetDataTable(SqlCommand cmd, string tablename)
        {
            cmd.CommandText = "SELECT * FROM " + tablename + " WHERE 1 = 0";
            SqlDataReader dataReader = cmd.ExecuteReader();
            var dt = new DataTable();
            dt.Load(dataReader);
            return dt;
        }

        protected void OnInfoMessage(object sender, SqlInfoMessageEventArgs args)
        {
            if (infoMessage.IsNull)
            {
                infoMessage = "";
            }
            infoMessage += args.Message + "\r\n";
        }

        public void assertEquals(String expectedString, String actualString)
        {
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = connection;
            cmd.CommandText = "tSQLt.AssertEqualsString";
            cmd.Parameters.AddWithValue("Expected", expectedString);
            cmd.Parameters.AddWithValue("Actual", actualString);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.ExecuteNonQuery();
        }

        public void failTestCaseAndThrowException(String failureMessage)
        {
            // tSQLt.Fail throws an exception which is uncaught and passed upwards
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = connection;
            cmd.CommandText = "tSQLt.Fail";
            cmd.Parameters.AddWithValue("Message0", failureMessage);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.ExecuteNonQuery();
        }

        public void logCapturedOutput(SqlString text)
        {
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = connection;
            cmd.CommandText = "tSQLt.LogCapturedOutput";
            cmd.Parameters.AddWithValue("text", text);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.ExecuteNonQuery();
        }
    }
}

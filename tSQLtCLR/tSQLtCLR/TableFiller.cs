using Microsoft.SqlServer.Server;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Text;

namespace tSQLtCLR
{
    class TableFiller
    {
        private TestDatabaseFacade testDatabaseFacade;
        private SqlBoolean faultTolerant = false;
        public TableFiller(TestDatabaseFacade testDatabaseFacade)
        {
            this.testDatabaseFacade = testDatabaseFacade;
        }

        public void SendResultSetToTables(SqlString tablesString, SqlString command, SqlBoolean? allowExtraColumns = null)
        {
            GetFaultTolerance(allowExtraColumns);
            var tablenames = ParseTableString(tablesString);
            var tgtColumns = testDatabaseFacade.getDataTableColumns(tablenames);
            List<DataTable> datatables = new List<DataTable>();
            var ResultsetCount = 0;
            using (SqlDataReader dataReader = testDatabaseFacade.executeCommand(command))
            {
                if (dataReader.FieldCount > 0)
                {
                    do
                    {
                        DataTable dt = null;

                        if (ResultsetCount < tablenames.Length)
                        {
                            dt = new DataTable();
                            dt.Load(dataReader); //this will call nextResult on the reader for us
                            dt.TableName = tablenames[ResultsetCount];//this has to happen after the load if no table was specified, it will result in the dt being named after the source table
                        } else
                        {
                            dataReader.NextResult();
                        }

                        datatables.Add(dt);
                        ResultsetCount++;
                    } while (dataReader.HasRows && ResultsetCount < tablenames.Length);
                }
            }

            ResultsetCount = 0;
            foreach (var table in datatables)
            {
                SendEachRecordOfData(table, tgtColumns[ResultsetCount]);
                ResultsetCount++;
            }
        }

        private void GetFaultTolerance(SqlBoolean? allowExtraColumns = null)
        {
            if (allowExtraColumns.HasValue)
            {
                faultTolerant = allowExtraColumns.Value;
                return;
            }
            SqlString sql = "If EXISTS (SELECT 1 FROM sys.objects where name = 'TestSettings') SELECT value FROM tSQLt.TestSettings where setting='ResultSetFillTable.AllowExtraColumns' else select '0' as value";
            using (SqlDataReader dataReader = testDatabaseFacade.executeCommand(sql))
            {
                if (dataReader.HasRows)
                {
                    while (dataReader.Read())
                    {
                        var allowString = dataReader.GetString(0);
                        if (allowString == "1" || allowString.Equals("true", StringComparison.InvariantCultureIgnoreCase))
                        {
                            faultTolerant = true;
                        }
                    }
                }
            }
        }

        private string[] ParseTableString(SqlString tablesString)
        {
            return tablesString.ToString().Split(',');
        }

        private void SendEachRecordOfData(DataTable table, List<string> targetCols)
        {
            if (!String.IsNullOrEmpty(table.TableName))
            {
                DataTable dtToSave = table;
                if (faultTolerant)
                {
                    var view = new DataView(table); //creates view over table
                    dtToSave = view.ToTable(table.TableName, false, targetCols.ToArray());
                }
                testDatabaseFacade.InsertTableData(dtToSave);
            }
        }

    }
}

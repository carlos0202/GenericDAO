using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.OleDb;
using System.Collections;
using System.Data.Common;

namespace GenericDAL
{
    /// <summary>
    /// 
    /// </summary>
    public class GenericDAO<TCommand, TConnection, TAdapter>
        where TCommand : DbCommand, new()
        where TConnection : DbConnection, new()
        where TAdapter : DbDataAdapter, new()
    {
        public TCommand Command { get; set; }
        private string connectionString;
        private DbTransaction transaction;
        private TConnection connection;
        private TAdapter adapter;
        private DataSet ds;
        private bool isTransaction;
        public CommandType commandType { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public GenericDAO()
        {
            connection = (TConnection)Activator.CreateInstance(typeof(TConnection), connectionString);
            commandType = CommandType.Text;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connString"></param>
        public GenericDAO(String connString)
        {
            this.connectionString = connString;
            connection = (TConnection)Activator.CreateInstance(typeof(TConnection), connString);
        }

        /// <summary>
        /// 
        /// </summary>
        public String ConnectionString
        {
            get { return connectionString; }
            set { connectionString = value; }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool IsTransaction
        {
            get { return isTransaction; }
        }

        /// <summary>
        /// 
        /// </summary>
        public void BeginTransaction()
        {
            if (!isTransaction && connection.State == ConnectionState.Open)
            {
                transaction = connection.BeginTransaction();
                isTransaction = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="level"></param>
        public void BeginTransaction(IsolationLevel level)
        {
            if (!isTransaction && connection.State == ConnectionState.Open)
            {
                transaction = connection.BeginTransaction(level);
                isTransaction = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void OpenConnection()
        {
            if (connection == null)
            {
                connection = new TConnection();
                connection.ConnectionString = connectionString;
            }
            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void CloseConnection()
        {
            if (connection.State == ConnectionState.Open)
            {
                connection.Close();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void CommitTransaction()
        {
            if (isTransaction && connection.State == ConnectionState.Open)
            {
                transaction.Commit();
                isTransaction = false;
            }
        }

        /// <summary>
        /// Executes a SQL statement that doesnt return any values.
        /// </summary>
        /// <param name="sqlCommand">Command to execute against database.</param>
        /// <param name="values">Values suplied as parameters to the command.</param>
        /// <param name="paramDirs">
        /// The direction of parameters when executing a stored procedure, following  the 
        /// integer representation of ParameterDirection enum.
        /// Input = 1,
        /// Output = 2,
        /// InputOutput = 3,
        /// ReturnValue = 6 (The parameter represents a return value from an operation such
        ///     as a stored procedure, built-in function, or user-defined function.)
        /// 
        /// </param>
        /// <returns>the integer value that returns from DbCommand ExecuteNonQuery method.</returns>
        public int ExecuteNonQuery(string sqlCommand, Object[] values, Object[] paramDirs = null)
        {
            this.LoadCommandObj(sqlCommand, values, paramDirs);

            return Command.ExecuteNonQuery();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public Object ExecuteScalar(string sqlCommand, object[] values, Object[] paramDirs = null)
        {
            Object returnVal = null;
            this.LoadCommandObj(sqlCommand, values, paramDirs);

            returnVal = Command.ExecuteScalar();

            return returnVal;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public DbDataReader ExecuteReader(string sqlCommand, object[] values, Object[] paramDirs = null)
        {
            this.LoadCommandObj(sqlCommand, values, paramDirs);

            return Command.ExecuteReader();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public DataSet ExecuteQuery(string sqlCommand, object[] values, Object[] paramDirs = null)
        {
            DataSet ds = new DataSet("DataTable");
            adapter = new TAdapter();
            adapter.TableMappings.Add("Table", "DataTable");
            this.LoadCommandObj(sqlCommand, values, paramDirs);

            adapter.SelectCommand = Command;
            adapter.Fill(ds);

            return ds;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="values"></param>
        /// <param name="dsName"></param>
        /// <returns></returns>
        public DataSet ExecuteNamedQuery(string sqlCommand, object[] values, string dsName, Object[] paramDirs = null)
        {
            adapter = new TAdapter();
            adapter.TableMappings.Add("Table", dsName);
            ds = new DataSet(dsName);
            this.LoadCommandObj(sqlCommand, values, paramDirs);

            adapter.SelectCommand = Command;
            adapter.Fill(ds, dsName);

            return ds;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public DbCommand FillCommand(String sqlCommand, Object[] values, Object[] paramDirs = null)
        {
            this.LoadCommandObj(sqlCommand, values, paramDirs);

            return Command;
        }

        private void LoadCommandObj(String sqlCommand, Object[] values, Object[] paramDirs = null)
        {
            Command = new TCommand();
            Command.Connection = connection;
            Command.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            Command.CommandType = commandType;
            int index = 0;

            if (values != null)
                foreach (Object val in values)
                {
                    if (paramDirs == null)
                    {
                        Command.AddWithValue(names[index].ToString(), val);
                    }
                    else
                    {
                        if (paramDirs.Length < values.Length && paramDirs.Contains(names[index]) && paramDirs[0] is String)
                        {
                            Command.AddWithValue(names[index].ToString(), val, ParameterDirection.Output);
                        }
                        else
                        {
                            Command.AddWithValue(names[index].ToString(), val, (ParameterDirection)paramDirs[index]);
                        }
                    }

                    index++;
                }
        }

    }
}

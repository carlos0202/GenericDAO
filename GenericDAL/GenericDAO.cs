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
        public TCommand commandObj { get; set; }
        private string connString;
        private DbTransaction transaction;
        private TConnection connectionObj;
        private TAdapter adapter;
        private DataSet ds;
        private bool isTransaction;
        public CommandType commandType { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public GenericDAO()
        {
            connString = DB.Default.DbDefaultUrl;
            connectionObj = (TConnection)Activator.CreateInstance(typeof(TConnection), connString);
            commandType = CommandType.Text;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connString"></param>
        public GenericDAO(String connString)
        {
            this.connString = connString;
            connectionObj = (TConnection)Activator.CreateInstance(typeof(TConnection), connString);
        }

        /// <summary>
        /// 
        /// </summary>
        public String ConnectionString
        {
            get { return connString; }
            set { connString = value; }
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
            if (!isTransaction && connectionObj.State == ConnectionState.Open)
            {
                transaction = connectionObj.BeginTransaction();
                isTransaction = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="level"></param>
        public void BeginTransaction(IsolationLevel level)
        {
            if (!isTransaction && connectionObj.State == ConnectionState.Open)
            {
                transaction = connectionObj.BeginTransaction(level);
                isTransaction = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void PrepareConnection()
        {
            if (connectionObj == null)
            {
                connectionObj = new TConnection();
                connectionObj.ConnectionString = connString;
            }
            if (connectionObj.State == ConnectionState.Closed)
            {
                connectionObj.Open();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void CloseConnection()
        {
            if (connectionObj.State == ConnectionState.Open)
            {
                connectionObj.Close();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void CommitTransaction()
        {
            if (isTransaction && connectionObj.State == ConnectionState.Open)
            {
                transaction.Commit();
                isTransaction = false;
            }
        }

        //version antigua del  metodo usando parametros indexados.
        /*
        public void ExecuteNonQuery(string command, Object [] Values)
        {
            using (OleDbConnection conn = this.ConexionBD)
            {
                comm = new OleDbCommand();
                comm.Connection = conn;
                comm.CommandText = command;
                comm.CommandType = CommandType.Text;
                IEnumerable<IDbDataParameter> Parameters = Values.Select(
                    (value, index) => DBUtils.ToConvertSqlParams(comm, index.ToString(), value)
                );
                comm.Parameters.AddRange(Parameters.ToArray());

                comm.ExecuteNonQuery();
            }
        }
        */

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
            commandObj = new TCommand();
            commandObj.Connection = connectionObj;
            commandObj.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            commandObj.CommandType = commandType;
            int index = 0;


            if (values != null)
                foreach (Object val in values)
                {
                    if (paramDirs == null)
                    {
                        commandObj.AddWithValue(names[index].ToString(), val);
                    }
                    else
                    {
                        commandObj.AddWithValue(names[index].ToString(), val, (ParameterDirection)paramDirs[index]);
                    }

                    index++;
                }

            return commandObj.ExecuteNonQuery();
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

            commandObj = new TCommand();
            commandObj.Connection = connectionObj;
            commandObj.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            commandObj.CommandType = commandType;
            int index = 0;

            if (values != null)
                foreach (Object val in values)
                {
                    if (paramDirs == null)
                    {
                        commandObj.AddWithValue(names[index].ToString(), val);
                    }
                    else
                    {
                        commandObj.AddWithValue(names[index].ToString(), val, (ParameterDirection)paramDirs[index]);
                    }

                    index++;
                }

            returnVal = commandObj.ExecuteScalar();

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
            commandObj = new TCommand();
            commandObj.Connection = connectionObj;
            commandObj.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            commandObj.CommandType = commandType;
            int index = 0;

            if (values != null)
                foreach (Object val in values)
                {
                    if (paramDirs == null)
                    {
                        commandObj.AddWithValue(names[index].ToString(), val);
                    }
                    else
                    {
                        commandObj.AddWithValue(names[index].ToString(), val, (ParameterDirection)paramDirs[index]);
                    }
                    index++;
                }

            return commandObj.ExecuteReader();
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

            commandObj = new TCommand();
            commandObj.Connection = connectionObj;
            commandObj.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            commandObj.CommandType = commandType;
            int index = 0;

            if (values != null)
                foreach (Object val in values)
                {
                    if (paramDirs == null)
                    {
                        commandObj.AddWithValue(names[index].ToString(), val);
                    }
                    else
                    {
                        commandObj.AddWithValue(names[index].ToString(), val, (ParameterDirection)paramDirs[index]);
                    }

                    index++;
                }

            adapter.SelectCommand = commandObj;
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
            commandObj = new TCommand();
            commandObj.Connection = connectionObj;
            commandObj.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            commandObj.CommandType = commandType;
            int index = 0;

            if (values != null)
                foreach (Object val in values)
                {
                    if (paramDirs == null)
                    {
                        commandObj.AddWithValue(names[index].ToString(), val);
                    }
                    else
                    {
                        commandObj.AddWithValue(names[index].ToString(), val, (ParameterDirection)paramDirs[index]);
                    }
                    index++;
                }

            adapter.SelectCommand = commandObj;
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
            commandObj = new TCommand();
            commandObj.Connection = connectionObj;
            commandObj.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            commandObj.CommandType = commandType;
            int index = 0;

            if (values != null)
                foreach (Object val in values)
                {
                    if (paramDirs == null)
                    {
                        commandObj.AddWithValue(names[index].ToString(), val);
                    }
                    else
                    {
                        commandObj.AddWithValue(names[index].ToString(), val, (ParameterDirection)paramDirs[index]);
                    }

                    index++;
                }

            return commandObj;
        }

        private void LoadCommandObj(String sqlCommand, Object[] values, Object[] paramDirs = null)
        {
            commandObj = new TCommand();
            commandObj.Connection = connectionObj;
            commandObj.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            commandObj.CommandType = commandType;
            int index = 0;

            if (values != null)
                foreach (Object val in values)
                {
                    if (paramDirs == null)
                    {
                        commandObj.AddWithValue(names[index].ToString(), val);
                    }
                    else
                    {
                        if (paramDirs.Length < values.Length && paramDirs.Contains(names[index]) && paramDirs[0] is String)
                        {
                            commandObj.AddWithValue(names[index].ToString(), val, ParameterDirection.Output);
                        }
                        else
                        {
                            commandObj.AddWithValue(names[index].ToString(), val, (ParameterDirection)paramDirs[index]);
                        }
                    }

                    index++;
                }
        }

    }
}

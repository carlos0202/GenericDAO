using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.OleDb;
using System.Collections;
using System.Data.Common;
using System.Data.OracleClient;

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
        private CommandType commandType;

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
        /// Internal helper to Rollback an active Transaction.
        /// </summary>
        public void RollBackTransaction()
        {
            if (connection.State == ConnectionState.Open && isTransaction)
            {
                transaction.Rollback();
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
        /// Executes a SQL stored procedure using the CommandType.StoredProcedure option.
        /// </summary>
        /// <param name="procedureName">Name of the procedure.</param>
        /// <param name="values">Values to supply for parameters, matching query and procedure declaration index.</param>
        /// <param name="parameterNames">Names for the parameters to build (comma[,] separated).</param>
        /// <param name="paramDirs">Parameter direcctions for the supplied parameters.</param>
        /// <returns>The integer value from DbCommand ExecuteNonQuery method.</returns>
        public int ExecuteNonQuery(string procedureName, Object[] values, String parameterNames, Object[] paramDirs = null)
        {
            this.LoadCommandObj(procedureName, values, paramDirs, true, parameterNames);

            return Command.ExecuteNonQuery();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public Object ExecuteScalar(string sqlCommand, Object[] values, Object[] paramDirs = null)
        {
            Object returnVal = null;
            this.LoadCommandObj(sqlCommand, values, paramDirs);

            returnVal = Command.ExecuteScalar();

            return returnVal;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="procedureName"></param>
        /// <param name="values"></param>
        /// <param name="parameterNames"></param>
        /// <param name="paramDirs"></param>
        /// <returns></returns>
        public Object ExecuteScalar(string procedureName, Object[] values, String parameterNames, Object[] paramDirs = null)
        {
            Object returnVal = null;
            this.LoadCommandObj(procedureName, values, paramDirs, true, parameterNames);
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

        public DataSet ExecuteQuery(string procedureName, Object[] values, string parameterNames, Object[] paramDirs = null)
        {
            DataSet ds = new DataSet("DataTable");
            adapter = new TAdapter();
            adapter.TableMappings.Add("Table", "DataTable");
            this.LoadCommandObj(procedureName, values, paramDirs, true, parameterNames);

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
        public DataSet ExecuteNamedQuery(string sqlCommand, Object[] values, string dsName, Object[] paramDirs = null)
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
        /// <param name="procedureName"></param>
        /// <param name="values"></param>
        /// <param name="dataSetName"></param>
        /// <param name="parameterNames"></param>
        /// <param name="paramDirs"></param>
        /// <returns></returns>
        public DataSet ExecuteNamedQuery(string procedureName, Object[] values, string dataSetName, string parameterNames, Object[] paramDirs = null)
        {
            adapter = new TAdapter();
            adapter.TableMappings.Add("Table", dataSetName);
            ds = new DataSet(dataSetName);
            this.LoadCommandObj(procedureName, values, paramDirs, true, parameterNames);
            adapter.SelectCommand = Command;
            adapter.Fill(ds, dataSetName);

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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="procedureName"></param>
        /// <param name="values"></param>
        /// <param name="parameterNames"></param>
        /// <param name="paramDirs"></param>
        /// <returns></returns>
        public DbCommand FillCommand(string procedureName, Object[] values, String parameterNames, Object[] paramDirs = null)
        {
            this.LoadCommandObj(procedureName, values, paramDirs, true, parameterNames);

            return Command;
        }

        private void LoadCommandObj(String sqlCommand, Object[] values, Object[] paramDirs = null, bool isProcedure = false, string parameters = null)
        {
            Command = new TCommand();
            Command.Connection = connection;
            Command.CommandText = sqlCommand;
            ArrayList names = new ArrayList();
            if (isProcedure)
            {
                names.AddRange(parameters.Split(','));
                Command.CommandType = CommandType.StoredProcedure;
            }
            else
            {
                names = DBUtils.getParameterNames<TCommand>(sqlCommand);
                Command.CommandType = commandType;
            }
             
            int index = 0;
            if (values != null)
            {
                foreach (Object val in values)
                {
                    if (paramDirs == null)
                    {
                        Command.AddWithValue(names[index].ToString(), val);
                    }
                    else
                    {
                        if (paramDirs.Contains(names[index]))
                        {
                            Command.AddWithValue(names[index].ToString(), val, ParameterDirection.Output);
                        }
                        else if (!paramDirs.Contains(names[index]) && paramDirs.Length < values.Length)
                        {
                            Command.AddWithValue(names[index].ToString(), val);
                        }
                        else
                        {
                            Command.AddWithValue(
                                names[index].ToString(), val,
                                (ParameterDirection)Enum.Parse(typeof(ParameterDirection),
                                paramDirs[index].ToString()));
                        }
                    }

                    index++;
                }
            }
        }

    }
}

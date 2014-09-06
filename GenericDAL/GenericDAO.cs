using System;
using System.Data;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data.OracleClient;
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
    public class GenericDAO<TProvider, TConnection>
        where TProvider:DbCommand, new() where TConnection: DbConnection, new()
    {
        private TProvider commandObj;
        private string connString;
        private DbTransaction transaction;
        private TConnection connectionObj;
        private DbDataAdapter adapter;
        private DataSet ds;
        private bool isTransaction;

        /// <summary>
        /// 
        /// </summary>
        public GenericDAO()
        {
            connString = DB.Default.DbDefaultUrl;
            connectionObj = (TConnection)Activator.CreateInstance(typeof(TConnection), connString);
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
            if (connectionObj.State == ConnectionState.Open && !isTransaction)
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
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public int ExecuteNonQuery(string sqlCommand, Object[] values)
        {
            commandObj = new TProvider();
            commandObj.Connection = connectionObj;
            commandObj.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            commandObj.CommandType = CommandType.Text;
            int index = 0;

            
            if (values != null)
                foreach (Object val in values)
                {
                    commandObj.AddWithValue(names[index++].ToString(), val);
                }

            return commandObj.ExecuteNonQuery();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public Object ExecuteScalar(string sqlCommand, object[] values)
        {
            Object returnVal = null;

            commandObj = new TProvider();
            commandObj.Connection = connectionObj;
            commandObj.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            commandObj.CommandType = CommandType.Text;
            int index = 0;

            if (values != null)
                foreach (Object val in values)
                {
                    commandObj.AddWithValue(names[index++].ToString(), val);
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
        public DbDataReader ExecuteReader(string sqlCommand, object[] values)
        {
            commandObj = new TProvider();
            commandObj.Connection = connectionObj;
            commandObj.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            commandObj.CommandType = CommandType.Text;
            int index = 0;

            if (values != null)
                foreach (Object val in values)
                {
                    commandObj.AddWithValue(names[index++].ToString(), val);
                }

            return commandObj.ExecuteReader();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public DataSet ExecuteQuery(string sqlCommand, object[] values)
        {
            DataSet ds = new DataSet("DataTable");
            DbDataAdapter da = GetAdapterInstance();
            da.TableMappings.Add("Table", "DataTable");

            commandObj = new TProvider();
            commandObj.Connection = connectionObj;
            commandObj.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            commandObj.CommandType = CommandType.Text;
            int index = 0;

            if(values != null)
                foreach (Object val in values)
                {
                    commandObj.AddWithValue(names[index++].ToString(), val);
                }

            da.SelectCommand = commandObj;
            da.Fill(ds);

            return ds;      
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="values"></param>
        /// <param name="dsName"></param>
        /// <returns></returns>
        public DataSet ExecuteNamedQuery(string sqlCommand, object[] values, string dsName)
        {
            adapter = GetAdapterInstance();
            adapter.TableMappings.Add("Table", dsName);
            ds = new DataSet(dsName);
            commandObj = new TProvider();
            commandObj.Connection = connectionObj;
            commandObj.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            commandObj.CommandType = CommandType.Text;
            int index = 0;

            if (values != null)
                foreach (Object val in values)
                {
                    commandObj.AddWithValue(names[index++].ToString(), val);
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
        public DbCommand FillCommand(String sqlCommand, Object[] values)
        {
            commandObj = new TProvider();
            commandObj.Connection = connectionObj;
            commandObj.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            commandObj.CommandType = CommandType.Text;
            int index = 0;

            if (values != null)
                foreach (Object val in values)
                {
                    commandObj.AddWithValue(names[index++].ToString(), val);
                }

            return commandObj;
        }

        public DbDataAdapter GetAdapterInstance()
        {
            if (typeof(TProvider) == typeof(SqlCommand))
            {
                return new SqlDataAdapter();
            }

            return new OracleDataAdapter();
        }

    }
}

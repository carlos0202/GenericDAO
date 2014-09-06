using System;
using System.Data;
using System.Collections.Generic;
using System.Data.SqlClient;
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
    public class GenericDAO
    {
        private SqlCommand comm;
        private string connString;
        private SqlTransaction transaction;
        private SqlConnection conn;
        private SqlDataAdapter adapter;
        private DataSet ds;
        private bool isTransaction;

        /// <summary>
        /// 
        /// </summary>
        public GenericDAO()
        {
            connString = DB.Default.MsDbUrl;
            conn = new SqlConnection(connString);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connString"></param>
        public GenericDAO(String connString)
        {
            this.connString = connString;
            conn = new SqlConnection(connString);
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
            if (!isTransaction && conn.State == ConnectionState.Open)
            {
                transaction = conn.BeginTransaction();
                isTransaction = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="level"></param>
        public void BeginTransaction(IsolationLevel level)
        {
            if (!isTransaction && conn.State == ConnectionState.Open)
            {
                transaction = conn.BeginTransaction(level);
                isTransaction = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void PrepareConnection()
        {
            if (conn == null)
            {
                conn = new SqlConnection(connString);
            }
            if (conn.State == ConnectionState.Closed)
            {
                conn.Open();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void CloseConnection()
        {
            if (conn.State == ConnectionState.Open && !isTransaction)
            {
                conn.Close();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void CommitTransaction()
        {
            if (isTransaction && conn.State == ConnectionState.Open)
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
            comm = new SqlCommand();
            comm.Connection = conn;
            comm.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            comm.CommandType = CommandType.Text;
            int index = 0;

            if (values != null)
                foreach (Object val in values)
                {
                    comm.Parameters.AddWithValue(names[index++].ToString(), val);
                }

            return comm.ExecuteNonQuery();
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

            comm = new SqlCommand();
            comm.Connection = conn;
            comm.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            comm.CommandType = CommandType.Text;
            int index = 0;

            if(values != null)
                foreach (Object val in values)
                {
                    comm.Parameters.AddWithValue(names[index++].ToString(), val);
                }

            returnVal = comm.ExecuteScalar();

            return returnVal;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public SqlDataReader ExecuteReader(string sqlCommand, object[] values)
        {
            comm = new SqlCommand();
            comm.Connection = conn;
            comm.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            comm.CommandType = CommandType.Text;
            int index = 0;

            if (values != null)
                foreach (Object val in values)
                {
                    comm.Parameters.AddWithValue(names[index++].ToString(), val);
                }

            return comm.ExecuteReader();
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
            SqlDataAdapter da = new SqlDataAdapter();
            da.TableMappings.Add("Table", "DataTable");

            comm = new SqlCommand();
            comm.Connection = conn;
            comm.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            comm.CommandType = CommandType.Text;
            int index = 0;

            if(values != null)
                foreach (Object val in values)
                {
                    comm.Parameters.AddWithValue(names[index++].ToString(), val);
                }

            da.SelectCommand = comm;
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
            adapter = new SqlDataAdapter();
            adapter.TableMappings.Add("Table", dsName);
            ds = new DataSet(dsName);
            comm = new SqlCommand();
            comm.Connection = conn;
            comm.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            comm.CommandType = CommandType.Text;
            int index = 0;

            if (values != null)
                foreach (Object val in values)
                {
                    comm.Parameters.AddWithValue(names[index++].ToString(), val);
                }

            adapter.SelectCommand = comm;
            adapter.Fill(ds, dsName);

            return ds;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public SqlCommand FillCommand(String sqlCommand, Object[] values)
        {
            comm = new SqlCommand();
            comm.Connection = conn;
            comm.CommandText = sqlCommand;
            ArrayList names = DBUtils.getParameterNames(sqlCommand);
            comm.CommandType = CommandType.Text;
            int index = 0;

            if (values != null)
                foreach (Object val in values)
                {
                    comm.Parameters.AddWithValue(names[index++].ToString(), val);
                }

            return comm;
        }

    }
}

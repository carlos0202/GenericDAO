using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace GenericDAL
{
    public static class DBUtils
    {
        public static IDbDataParameter ToConvertSqlParams(IDbCommand command, string name, object value)
        {
            var p = command.CreateParameter();
            p.ParameterName = name;
            p.Value = value;
            return p;
        }

        public static ArrayList getParameterNames(string query)
        {
            Regex pattern = new Regex(@"(?<!@)@\w+");
            ArrayList paramNames = new ArrayList();
            foreach(Match match in pattern.Matches(query))
            {
                paramNames.Add(match.Value);
            }
            
            return paramNames;
        }

        /// <summary>
        /// Adds a parameter to the command.
        /// </summary>
        /// <param name="comm">
        /// The command object.
        /// </param>
        /// <param name="paramName">
        /// The name of the parameter.
        /// </param>
        /// <param name="value">
        /// The parameter value to add.
        /// </param>
        /// <remarks>
        /// </remarks>
        public static void AddWithValue<TCommand>(this TCommand comm, string paramName, object value, ParameterDirection direction = ParameterDirection.Input)
            where TCommand: DbCommand
        {
            var param = comm.CreateParameter();
            param.ParameterName = paramName;
            param.Value = value;
            param.Direction = direction;
            comm.Parameters.Add(param);
        }
    }
}

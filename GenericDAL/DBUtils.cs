using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace GenericDAL
{
    public class DBUtils
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
    }
}

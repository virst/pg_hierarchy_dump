using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using CommandLine;
using Npgsql;

namespace pg_hierarchy_dump
{
    class Program
    {
        private const string ConFile = "pg_hierarchy_dump.con";

        static string[] SqlLine(object o)
        {
            var s = o.ToString();
            if (!string.IsNullOrWhiteSpace(s)) s += (";" + Environment.NewLine + Environment.NewLine);
            return s.Split(Environment.NewLine.ToCharArray());
        }

        static void Main(string[] args)
        {
            var pr = Parser.Default.ParseArguments<Options>(args);
            if(pr.Tag == ParserResultType.NotParsed) return;
            var o = ((Parsed<Options>) pr).Value;

            try
            {
                string conStr = o.ConnString??File.ReadAllText(ConFile, Encoding.UTF8);
                var ss = o.DbObject.Split('.');
                string shema = ss[0];
                string obj = ss[1];

                List<string> lines = new List<string>();

                Console.WriteLine("DB Object - {0}", args[0]);

                using (var con = new NpgsqlConnection(conStr))
                {
                    con.Open();
                    Console.WriteLine("NpgsqlConnection Open - OK");
                    using (var comm = con.CreateCommand())
                    {
                        comm.CommandText = Sql(shema, obj);


                        using (var reader = comm.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Console.Write("{0} - {1}.{2}", reader["obj_type"], reader["obj_schema"], reader["obj_name"]);
                                lines.Add($"--- begin {reader["obj_schema"]}.{reader["obj_name"]}");

                                lines.AddRange(SqlLine(reader["exec"]));
                                lines.AddRange(SqlLine(reader["exec_grant"]));
                                lines.AddRange(SqlLine(reader["exec_COMMENT"]));
                                lines.AddRange(SqlLine(reader["exec_COMMENT_col"]));

                                lines.Add($"--- end {reader["obj_schema"]}.{reader["obj_name"]}");
                                Console.WriteLine("+");
                            }
                        }
                    }
                }

                for (int i = 1; i < lines.Count;)
                {
                    if (string.IsNullOrWhiteSpace(lines[i - 1]) && string.IsNullOrWhiteSpace(lines[i]))
                        lines.RemoveAt(i);
                    else
                        i++;
                }

                Console.Write("File Write ");
                File.WriteAllLines(o.FileName??(args[0] + ".sql"), lines, Encoding.UTF8);
                Console.WriteLine("+");
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex);
            }
        }

        #region Query Generation

        static string Sql(string shema, string obj) =>
            @"select v_curr.obj_schema, v_curr.obj_name, v_curr.obj_type, COALESCE( viw.exec_view, m_viw.exec_view) as exec , grnt.exec_grant, com.exec_COMMENT, com_col.exec_COMMENT_col  " +
            Environment.NewLine +
            @"from ( " + Environment.NewLine +
            @"select obj_schema, obj_name, obj_type , max(depth) as dm from" + Environment.NewLine +
            @"  (" + Environment.NewLine +
            @"  with recursive recursive_deps(obj_schema, obj_name, obj_type, depth) as " + Environment.NewLine +
            @"  (" + Environment.NewLine +
            $@"    select '{shema}'::varchar, '{obj}'::varchar, null::varchar, 0" + Environment.NewLine +
            @"    union" + Environment.NewLine +
            @"    select dep_schema::varchar, dep_name::varchar, dep_type::varchar, recursive_deps.depth + 1 from " +
            Environment.NewLine +
            @"    (" + Environment.NewLine +
            @"      select ref_nsp.nspname ref_schema, ref_cl.relname ref_name, " + Environment.NewLine +
            @"      rwr_cl.relkind dep_type," + Environment.NewLine +
            @"      rwr_nsp.nspname dep_schema," + Environment.NewLine +
            @"      rwr_cl.relname dep_name" + Environment.NewLine +
            @"      from pg_depend dep" + Environment.NewLine +
            @"      join pg_class ref_cl on dep.refobjid = ref_cl.oid" + Environment.NewLine +
            @"      join pg_namespace ref_nsp on ref_cl.relnamespace = ref_nsp.oid" + Environment.NewLine +
            @"      join pg_rewrite rwr on dep.objid = rwr.oid" + Environment.NewLine +
            @"      join pg_class rwr_cl on rwr.ev_class = rwr_cl.oid" + Environment.NewLine +
            @"      join pg_namespace rwr_nsp on rwr_cl.relnamespace = rwr_nsp.oid" + Environment.NewLine +
            @"      where dep.deptype = 'n'" + Environment.NewLine +
            @"      and dep.classid = 'pg_rewrite'::regclass" + Environment.NewLine +
            @"    ) deps" + Environment.NewLine +
            @"    join recursive_deps on deps.ref_schema = recursive_deps.obj_schema and deps.ref_name = recursive_deps.obj_name" +
            Environment.NewLine +
            @"    where (deps.ref_schema != deps.dep_schema or deps.ref_name != deps.dep_name)" + Environment.NewLine +
            @"    )" + Environment.NewLine +
            @"  select obj_schema, obj_name, obj_type, depth" + Environment.NewLine +
            @"  from recursive_deps " + Environment.NewLine +
            @"  where depth > 0" + Environment.NewLine +
            @"  ) t " + Environment.NewLine +
            @"  group by obj_schema, obj_name, obj_type" + Environment.NewLine +
            @"  order by max(depth) desc) v_curr" + Environment.NewLine +
            @"  left join (select nspname, " + Environment.NewLine +
            @"         relname, " + Environment.NewLine +
            @"         string_agg( COALESCE( 'COMMENT ON ' ||" + Environment.NewLine +
            @"                   case" + Environment.NewLine +
            @"                       when c.relkind = 'v' then 'VIEW'" + Environment.NewLine +
            @"                       when c.relkind = 'm' then 'MATERIALIZED VIEW'" + Environment.NewLine +
            @"                       else ''" + Environment.NewLine +
            @"                   end" + Environment.NewLine +
            @"                   || ' ' || n.nspname || '.' || c.relname || ' IS ''' || replace(d.description, '''', '''''') || ''';'" +
            Environment.NewLine +
            @"                   , 'Null value COMMENT ON' )" + Environment.NewLine +
            @"                   , E'\n' ) as exec_COMMENT" + Environment.NewLine +
            @"  from pg_class c" + Environment.NewLine +
            @"  join pg_namespace n on n.oid = c.relnamespace" + Environment.NewLine +
            @"  join pg_description d on d.objoid = c.oid and d.objsubid = 0" + Environment.NewLine +
            @"  group by nspname, relname) com " + Environment.NewLine +
            @"  on com.nspname = v_curr.obj_schema and com.relname = v_curr.obj_name" + Environment.NewLine +
            @"  " + Environment.NewLine +
            @"  left join (select nspname, " + Environment.NewLine +
            @"         relname,  " + Environment.NewLine +
            @"         string_agg(" + Environment.NewLine +
            @"                    COALESCE('COMMENT ON COLUMN ' || n.nspname || '.' || c.relname || '.' || a.attname || ' IS ''' || replace(d.description, '''', '''''') || ''';'" +
            Environment.NewLine +
            @"                  , 'Null value COMMENT ON COLUMN' )" + Environment.NewLine +
            @"         , E'\n' ) as exec_COMMENT_col" + Environment.NewLine +
            @"  from pg_class c" + Environment.NewLine +
            @"  join pg_attribute a on c.oid = a.attrelid" + Environment.NewLine +
            @"  join pg_namespace n on n.oid = c.relnamespace" + Environment.NewLine +
            @"  join pg_description d on d.objoid = c.oid and d.objsubid = a.attnum" + Environment.NewLine +
            @"  group by nspname, relname) com_col" + Environment.NewLine +
            @"  on com_col.nspname = v_curr.obj_schema and com_col.relname = v_curr.obj_name" + Environment.NewLine +
            @"  " + Environment.NewLine +
            @"  Left join (select table_schema, " + Environment.NewLine +
            @"         table_name,   " + Environment.NewLine +
            @"         string_agg(" + Environment.NewLine +
            @"                    COALESCE('GRANT ' || privilege_type || ' ON ' || table_schema || '.' || table_name || ' TO ""' || grantee || '""'" +
            Environment.NewLine +
            @"                  ,'Null value GRANT' )" + Environment.NewLine +
            @"         , E'\n' ) as exec_grant" + Environment.NewLine +
            @"  from information_schema.role_table_grants " + Environment.NewLine +
            @"  group by table_schema, table_name) grnt" + Environment.NewLine +
            @"  on grnt.table_schema = v_curr.obj_schema and grnt.table_name = v_curr.obj_name" + Environment.NewLine +
            @"  " + Environment.NewLine +
            @"  left join (select table_schema, " + Environment.NewLine +
            @"           table_name, " + Environment.NewLine +
            @"           string_agg(" + Environment.NewLine +
            @"                      COALESCE( 'CREATE VIEW ' || table_schema || '.' || table_name || ' AS ' || view_definition" +
            Environment.NewLine +
            @"                    ,'Null value CREATE VIEW ')" + Environment.NewLine +
            @"           , E'\n' ) as exec_view" + Environment.NewLine +
            @"    from information_schema.views   " + Environment.NewLine +
            @"    group by table_schema, table_name) viw" + Environment.NewLine +
            @"    on viw.table_schema = v_curr.obj_schema and viw.table_name = v_curr.obj_name" + Environment.NewLine +
            @"    " + Environment.NewLine +
            @"    left join (select schemaname, " + Environment.NewLine +
            @"           matviewname, " + Environment.NewLine +
            @"           string_agg(" + Environment.NewLine +
            @"                      COALESCE( 'CREATE MATERIALIZED VIEW ' || schemaname || '.' || matviewname || ' AS ' || definition" +
            Environment.NewLine +
            @"                    ,'Null value CREATE MATERIALIZED VIEW ')" + Environment.NewLine +
            @"           , E'\n' ) as exec_view" + Environment.NewLine +
            @"    from pg_matviews   " + Environment.NewLine +
            @"    group by schemaname, matviewname) m_viw" + Environment.NewLine +
            @"    on m_viw.schemaname = v_curr.obj_schema and m_viw.matviewname = v_curr.obj_name " +
            Environment.NewLine +
            @"order by dm " + Environment.NewLine +
            @"  ";

        #endregion
    }
}

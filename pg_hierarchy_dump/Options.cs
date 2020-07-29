using CommandLine;

namespace pg_hierarchy_dump
{
    class Options
    {
        [Value(0,Required = true,HelpText = "DB object name")]
        public string DbObject { get; set; }

        [Option('c', "connstr", Required = false, HelpText = "Connection string")]
        public string ConnString { get; set; }
        [Option('o', "output", Required = false, HelpText = "Output file")]
        public string FileName { get; set; }
    }
}

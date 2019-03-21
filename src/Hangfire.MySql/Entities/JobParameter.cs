namespace Hangfire.MySql.Entities
{
    internal class JobParameter
    {
        public long JobId { get; set; }
        public string Name { get; set; }
        public string Value { get; set; }
    }
}

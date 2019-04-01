﻿using System;

namespace Hangfire.MySql.Entities
{
    public class Server
    {
        public string Id { get; set; }
        public string Data { get; set; }
        public DateTime LastHeartbeat { get; set; }
    }
}

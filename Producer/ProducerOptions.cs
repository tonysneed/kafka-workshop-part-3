﻿namespace Producer
{
    public class ProducerOptions
    {
        public string Brokers { get; set; }
        public string RawTopic { get; set; }
        public string SchemaRegistryUrl { get; set; }
    }
}

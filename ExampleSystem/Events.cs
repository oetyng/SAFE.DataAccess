using SAFE.DataAccess;
using System;

namespace ExampleSystem
{
    public class StoredEvent
    {
        public static StoredEvent From(Event e, string streamName, Guid streamId)
        {
            return new StoredEvent
            {
                Id = e.Id,
                Payload = e.Json(),
                EventName = e.GetType().Name,
                SequenceNr = e.SequenceNr,
                StreamKey = $"{streamName}@{streamId}",
                StreamId = streamId,
                StreamName = streamName,
                TimeStamp = e.TimeStamp
            };
        }

        public Guid Id { get; set; }
        public string Payload { get; set; }
        public string EventName { get; set; }
        public int SequenceNr { get; set; }
        public string StreamKey { get; set; }
        public Guid StreamId { get; set; }
        public string StreamName { get; set; }
        public DateTime TimeStamp { get; set; }

        public override string ToString()
        {
            var sb = new System.Text.StringBuilder();
            sb.AppendLine($"{SequenceNr}@{EventName}: {TimeStamp}");
            sb.AppendLine(Payload);
            sb.AppendLine();
            return sb.ToString();
        }
    }

    public class Event
    {
        public Guid Id { get; set; }
        public int SequenceNr { get; set; }
        public DateTime TimeStamp { get; set; }
        public Event()
        {
            Id = Guid.NewGuid();
            TimeStamp = DateTime.UtcNow;
        }
    }

    class EShopInitiated : Event
    {
        public EShopInitiated(Guid eShopId, string name)
        {
            EShopId = eShopId;
            Name = name;
        }
        public Guid EShopId { get; set; }
        public string Name { get; set; }
    }

    class ProductTypeAdded : Event
    {
        public ProductTypeAdded(Guid eShopId, string productType, int quantity)
        {
            EShopId = eShopId;
            ProductType = productType;
            Quantity = quantity;
        }
        public Guid EShopId { get; set; }
        public string ProductType { get; set; }
        public int Quantity { get; set; }
    }

    class StockRefilled : Event
    {
        public StockRefilled(Guid eShopId, string productType, int quantity)
        {
            EShopId = eShopId;
            ProductType = productType;
            Quantity = quantity;
        }
        public Guid EShopId { get; set; }
        public string ProductType { get; set; }
        public int Quantity { get; set; }
    }

    class ItemsSold : Event
    {
        public ItemsSold(Guid eShopId, string productType, int quantity)
        {
            EShopId = eShopId;
            ProductType = productType;
            Quantity = quantity;
        }
        public Guid EShopId { get; set; }
        public string ProductType { get; set; }
        public int Quantity { get; set; }
    }

    class ItemsLostOrDamaged : Event
    {
        public ItemsLostOrDamaged(Guid eShopId, string productType, int quantity)
        {
            EShopId = eShopId;
            ProductType = productType;
            Quantity = quantity;
        }
        public Guid EShopId { get; set; }
        public string ProductType { get; set; }
        public int Quantity { get; set; }
    }
}

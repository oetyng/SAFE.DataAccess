using System;
using System.Collections.Generic;

namespace ExampleSystem
{
    class EShopState
    {
        public int Version = -1;
        public Guid Id { get; set; }
        public string Name { get; set; }
        public Dictionary<string, int> ProductTypeQuantity { get; set; } = new Dictionary<string, int>();

        public override string ToString()
        {
            var sb = new System.Text.StringBuilder();
            sb.AppendLine($"{Name}, number of events: {Version + 1}");
            sb.AppendLine($"Product quantities:");
            foreach (var pair in ProductTypeQuantity)
                sb.AppendLine($"{pair.Key} : {pair.Value}");
            return sb.ToString();
        }
    }

    class EShop
    {
        public readonly EShopState State = new EShopState();

        public List<Event> InitShop(string name)
        {
            if (State.Id != Guid.Empty)
                return Nothing();
            var id = name.ToGuid();
            return Package(new EShopInitiated(id, name));
        }

        public List<Event> AddProductType(string productType, int quantity)
        {
            if (State.ProductTypeQuantity.ContainsKey(productType))
                return Nothing();
            return Package(new ProductTypeAdded(State.Id, productType, quantity));
        }

        public List<Event> RefillStock(string productType, int quantity)
        {
            if (Valid(productType))
                return Package(new StockRefilled(State.Id, productType, quantity));
            else
                return Nothing();
        }

        public List<Event> SellItems(string productType, int quantity)
        {
            if (Valid(productType))
                return Package(new ItemsSold(State.Id, productType, quantity));
            else
                return Nothing();
        }

        public List<Event> ReportStockLossOrDamage(string productType, int quantity)
        {
            if (Valid(productType))
                return Package(new ItemsLostOrDamaged(State.Id, productType, quantity));
            else
                return Nothing();
        }

        public void Apply(List<Event> events)
        {
            foreach (var e in events)
            {
                Apply((dynamic)e);
                State.Version++;
                e.SequenceNr = State.Version;
            }
        }

        bool Valid(string productType)
        {
            if (!State.ProductTypeQuantity.ContainsKey(productType))
            {
                Console.WriteLine("Product type does not exist!");
                return false;
            }
            return true;
        }

        List<Event> Nothing()
        {
            return new List<Event>();
        }

        List<Event> Package(Event e)
        {
            return new List<Event> { e };
        }

        void Apply(EShopInitiated e)
        {
            State.Id = e.EShopId;
            State.Name = e.Name;
        }

        void Apply(ProductTypeAdded e)
        {
            State.ProductTypeQuantity[e.ProductType] = e.Quantity;
        }

        void Apply(StockRefilled e)
        {
            State.ProductTypeQuantity[e.ProductType] += e.Quantity;
        }

        void Apply(ItemsSold e)
        {
            State.ProductTypeQuantity[e.ProductType] -= e.Quantity;
        }

        void Apply(ItemsLostOrDamaged e)
        {
            State.ProductTypeQuantity[e.ProductType] -= e.Quantity;
        }
    }
}

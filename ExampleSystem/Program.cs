using SAFE.DataAccess;
using System;
using System.Collections.Generic;

namespace ExampleSystem
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("This is your EShop!");
            MdAccess.UseInMemoryDb();

            var pwd = GetPwd();
            var db = GetDatabase(pwd);
            var eShop = AddEShop(db);

            Console.WriteLine("Choose an option:");
            ListOptions();
            ConsoleKeyInfo input;

            do
            {
                input = Console.ReadKey();
                switch(input.Key)
                {
                    case ConsoleKey.D1:
                        AddProduct(eShop, db);
                        break;
                    case ConsoleKey.D2:
                        RefillStock(eShop, db);
                        break;
                    case ConsoleKey.D3:
                        SellItems(eShop, db);
                        break;
                    case ConsoleKey.D4:
                        ReportLossOrDamage(eShop, db);
                        break;
                    case ConsoleKey.D5:
                        ShowCurrentState(eShop);
                        break;
                    case ConsoleKey.D6:
                        ShowHistory(db);
                        break;
                    case ConsoleKey.F1:
                        ListOptions();
                        break;
                    case ConsoleKey.Escape:
                        return;
                    default:
                        break;
                }

                Console.WriteLine("Choose again or ESC to cancel. F1 to list options.");

            } while (input.Key != ConsoleKey.Escape);
        }

        static void ShowCurrentState(EShop eShop)
        {
            Console.WriteLine(". Current state");
            Console.WriteLine(eShop.State);
        }

        static void ShowHistory(Database db)
        {
            Console.WriteLine(". Event history");
            var events = db.GetAllAsync<StoredEvent>().GetAwaiter().GetResult();
            foreach (var e in events)
                Console.WriteLine(e);
        }

        static void ListOptions()
        {
            Console.WriteLine("1. Add product type, 2. Refill product stock, 3. Sell item");
            Console.WriteLine("4. Report loss or damage, 5. Show current state, 6. Show history");
            Console.WriteLine();
        }

        static void AddProduct(EShop eShop, Database db)
        {
            Console.WriteLine(". Add product");
            var (product, qty) = GetProductQuantity();

            var events = eShop.AddProductType(product, qty);
            Save(events, eShop, db);
        }

        static void RefillStock(EShop eShop, Database db)
        {
            Console.WriteLine(". Refill stock");
            var (product, qty) = GetProductQuantity();

            var events = eShop.RefillStock(product, qty);
            Save(events, eShop, db);
        }

        static void SellItems(EShop eShop, Database db)
        {
            Console.WriteLine(". Sell items");
            var (product, qty) = GetProductQuantity();

            var events = eShop.SellItems(product, qty);
            Save(events, eShop, db);
        }

        static void ReportLossOrDamage(EShop eShop, Database db)
        {
            Console.WriteLine(". Report loss or damage");
            var (product, qty) = GetProductQuantity();

            var events = eShop.ReportStockLossOrDamage(product, qty);
            Save(events, eShop, db);
        }

        static (string, int) GetProductQuantity()
        {
            Console.WriteLine("Enter product name:");
            var product = string.Empty;
            do
            {
                product = Console.ReadLine();
            } while (string.IsNullOrEmpty(product) || string.IsNullOrWhiteSpace(product));
            Console.WriteLine("Enter product quantity:");
            var qty = 0;
            var input = string.Empty;
            do
            {
                input = Console.ReadLine();
            } while (!int.TryParse(input, out qty) || 0 > qty);
            return (product, qty);
        }

        static void Save(List<Event> events, EShop eShop, Database db)
        {
            eShop.Apply(events);
            events.ForEach(e =>
            {
                var stored = StoredEvent.From(e, eShop.State.Name, eShop.State.Id);
                db.AddAsync(stored.Id.ToString(), stored).GetAwaiter().GetResult();
            });
        }

        static Database GetDatabase(string pwd)
        {
            var indexer = Indexer.CreateAsync(pwd).GetAwaiter().GetResult();
            var dbResult = Database.GetOrAddAsync(pwd, indexer).GetAwaiter().GetResult();
            return dbResult.Value;
        }

        static string GetPwd()
        {
            Console.WriteLine("Please enter pwd:");
            string pass = "";
            do
            {
                ConsoleKeyInfo key = Console.ReadKey(true);
                // Backspace Should Not Work
                if (key.Key != ConsoleKey.Backspace && key.Key != ConsoleKey.Enter)
                {
                    pass += key.KeyChar;
                    Console.Write("*");
                }
                else
                {
                    if (key.Key == ConsoleKey.Backspace && pass.Length > 0)
                    {
                        pass = pass.Substring(0, (pass.Length - 1));
                        Console.Write("\b \b");
                    }
                    else if (key.Key == ConsoleKey.Enter)
                    {
                        break;
                    }
                }
            } while (true);

            Console.WriteLine();
            Console.WriteLine("Oh lala, very strong pwd..!");
            Console.WriteLine();

            return pass;
        }

        static EShop AddEShop(Database db)
        {
            var eShop = new EShop();
            var events = eShop.InitShop("EShop");
            Save(events, eShop, db);
            return eShop;
        }
    }
}

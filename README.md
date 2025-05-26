# DatafordelerConverter

> **A .NET 9 utility for converting Danish address and property data (DAR, MAT) to CSV files.**  
> Efficiently processes large JSON exports from Datafordeler and outputs normalized CSVs for further integration into Pofos database.

---

## 🚀 Features

- **Streams large JSON files** for low memory usage
- **Exports:**
  - Road names (`RoadName.csv`)
  - Postcodes (`PostCode.csv`)
  - Address access (`AddressAccess.csv`)
  - Address specifics (`AddressSpecific.csv`)
- **Enriches address data** with cadastral and geospatial information
- **Configurable** via `appsettings.json`

---

## 🛠 Prerequisites

- [.NET 9 SDK](https://dotnet.microsoft.com/)
- [Newtonsoft.Json](https://www.nuget.org/packages/Newtonsoft.Json/) NuGet package

---

## ⚙️ Configuration

Edit your `appsettings.json` to specify input and output file paths.

---

## 📦 Output Files

| File                  | Description                |
|-----------------------|---------------------------|
| `RoadName.csv`        | Road names                |
| `PostCode.csv`        | Postcodes                 |
| `AddressAccess.csv`   | Address access data       |
| `AddressSpecific.csv` | Address specifics         |

---

## 📁 Project Structure

- `Program.cs` – Main entry point, orchestrates the conversion
- `AddressAccess.cs`, `RoadName.cs`, `PostCode.cs`, etc. – Data processing logic
- `MatDataLoader.cs`, `CommonDataLoader.cs` – Lookup and enrichment helpers
- `appsettings.json` – Configuration file

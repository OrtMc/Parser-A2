using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using System.Net.Http;
using Nancy.Json;
using System.Net.Http.Headers;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Text.RegularExpressions;

public class Firm
{
    public string sellerName;
    public string sellerInn;
    public string buyerName;
    public string buyerInn;
    public string woodVolumeBuyer;
    public string woodVolumeSeller;
    public string dealDate;
    public string dealNumber;
}

public class Body
{
    public string query;
    public Variables variables;
    public string operationName;
}

public class Variables
{
    public int size;
    public int number;
    public Orders orders;
}

public class Orders
{
    public string property;
    public string direction;
}

namespace Parser_A2
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            SqlConnection sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["A2DB"].ConnectionString);

            sqlConnection.Open();

            if (sqlConnection.State == ConnectionState.Open)
            {
                Console.WriteLine("Успешное подключение к базе данных.");
            }
            else
            {
                Console.WriteLine("Подключение к базе данных не установлено.");
                return;
            }

            Body postBody = new Body();
            postBody.query = "query SearchReportWoodDeal($size: Int!, $number: Int!, $orders: [Order!]) {\n  searchReportWoodDeal(, pageable: {number: $number, size: $size}, orders: $orders) {\n    content {\n      sellerName\n      sellerInn\n      buyerName\n      buyerInn\n      woodVolumeBuyer\n      woodVolumeSeller\n      dealDate\n      dealNumber\n}\n}\n}\n";
            postBody.variables = new Variables();
            postBody.variables.orders = new Orders();
            postBody.variables.orders.property = "dealDate";
            postBody.variables.orders.direction = "ASC";
            postBody.operationName = "SearchReportWoodDeal";

            bool success;

            while (true){ 
                success = await HandleData(sqlConnection, postBody);

                if (success)
                {
                    Console.WriteLine("Данные с сайта проверены.");
                    await Task.Delay(1 * 60 * 1000);
                }
                else Console.WriteLine("Не удалось проверить данные с сайта");
            }

            // Console.ReadKey();
        }

        private static async Task<bool> HandleData(SqlConnection sqlConnection, Body postBody)
        {
            Console.WriteLine("Начало проверки данных с сайта.");
            bool success = await GetDataAndInsertIntoDataBase(postBody, sqlConnection);
            return success;
        }

        private static async Task<bool> GetDataAndInsertIntoDataBase(Body postBody, SqlConnection sqlConnection)
        {
            int iter = 0;
            string postResult;
            JObject jsonResult;
            Firm firm;
            Firm firmInDatabase;
            while (true)
            {
                try
                {
                    postResult = GetData(url: "https://www.lesegais.ru/open-area/graphql", postBody, 200, iter);
                    if (postResult == null) return false;

                        if (JObject.Parse(postResult)["data"]["searchReportWoodDeal"]["content"].ToArray().Length == 0)
                    {
                        Console.WriteLine("Достигнут конец списка.");
                        break;
                    }

                    jsonResult = JObject.Parse(postResult);

                    foreach (var item in jsonResult["data"]["searchReportWoodDeal"]["content"])
                    {
                        firm = await ValidateFirm(item.ToObject<Firm>());

                        firmInDatabase =  await GetFirmFromDatabase(firm, sqlConnection);
                        if (firmInDatabase.dealNumber == null)
                        {
                            await InsertFirmAsync(firm, sqlConnection);
                        }
                        else if(firm.sellerName != firmInDatabase.sellerName
                            || firm.sellerInn != firmInDatabase.sellerInn
                            || firm.buyerName != firmInDatabase.buyerName
                            || firm.buyerInn != firmInDatabase.buyerInn
                            || firm.woodVolumeBuyer != firmInDatabase.woodVolumeBuyer
                            || firm.woodVolumeSeller != firmInDatabase.woodVolumeSeller
                            || firm.dealDate != firmInDatabase.dealDate
                            || firm.dealNumber != firmInDatabase.dealNumber)
                        {
                            await UpdateFirmAsync(firm, sqlConnection);
                        }
                        
                    }

                    iter++;
                    if (iter == 10) break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Не удалось вставить/обновить данные в базе.");
                    Console.WriteLine(ex.Message);
                    continue;
                }
            }
            return true;
        }

        private static async Task<Firm> ValidateFirm(Firm firm)
        {
            Firm result = new Firm();

            Regex regexBuyerName = new Regex(@"[\?\$\{\}\&]");
            result.buyerName = firm.buyerName==null?"":regexBuyerName.Replace(firm.buyerName, "");
            Regex regexBuyerInn = new Regex(@"[^\d]");
            result.buyerInn = firm.buyerInn == null ? "" : regexBuyerInn.Replace(firm.buyerInn, "");
            Regex regexSellerName = new Regex(@"[\?\$\{\}\&]");
            result.sellerName = firm.sellerName == null ? "" : regexSellerName.Replace(firm.sellerName, "");
            Regex regexSellerInn = new Regex(@"[^\d]");
            result.sellerInn = firm.sellerInn == null ? "" : regexSellerInn.Replace(firm.sellerInn, "");
            Regex regexWoodVolumeSeller = new Regex(@"[^\d\.]");
            result.woodVolumeSeller = firm.woodVolumeSeller == null ? "" : regexWoodVolumeSeller.Replace(firm.woodVolumeSeller, "");
            Regex regexWoodVolumeBuyer = new Regex(@"[^\d\.]");
            result.woodVolumeBuyer = firm.woodVolumeBuyer == null ? "" : regexWoodVolumeBuyer.Replace(firm.woodVolumeBuyer, "");
            Regex regexDealDate = new Regex(@"[^\d\-]");
            result.dealDate = firm.dealDate == null ? "" : regexDealDate.Replace(firm.dealDate, "");
            Regex regexDealNumber = new Regex(@"[^\d]");
            result.dealNumber = firm.dealNumber == null ? "" : regexDealNumber.Replace(firm.dealNumber, "");

            return result;
        }

        private static async Task UpdateFirmAsync(Firm firm, SqlConnection sqlConnection)
        {
            SqlCommand sqlCommand = new SqlCommand(
                $"UPDATE Firms SET sellerName = N'{firm.sellerName}', sellerInn = N'{firm.sellerInn}', buyerName = '{firm.buyerName}', buyerInn = '{firm.buyerInn}', woodVolumeBuyer = '{firm.woodVolumeBuyer}', woodVolumeSeller = '{firm.woodVolumeSeller}', dealDate = '{firm.dealDate}', dealNumber = '{firm.dealNumber}' WHERE dealNumber = '{firm.dealNumber}' AND dealDate = '{firm.dealDate}'",
                sqlConnection);
            await sqlCommand.ExecuteNonQueryAsync();
        }

        private static async Task<Firm> GetFirmFromDatabase(Firm firm, SqlConnection sqlConnection)
        {
            SqlDataReader dataReader = null;
            Firm result = new Firm();

            try
            {
                SqlCommand sqlCommand = new SqlCommand(
                $"SELECT sellerName, sellerInn, buyerName, buyerInn, woodVolumeBuyer, woodVolumeSeller, dealDate, dealNumber FROM Firms WHERE dealNumber = '{firm.dealNumber}' AND dealDate = '{firm.dealDate}'",
                sqlConnection);
                dataReader = await sqlCommand.ExecuteReaderAsync();
                while (dataReader.Read())
                {
                    result.sellerName = dataReader["sellerName"].ToString();
                    result.sellerInn = dataReader["sellerInn"].ToString();
                    result.buyerName = dataReader["buyerName"].ToString();
                    result.buyerInn = dataReader["buyerInn"].ToString();
                    result.woodVolumeBuyer = dataReader["woodVolumeBuyer"].ToString();
                    result.woodVolumeSeller = dataReader["woodVolumeSeller"].ToString();
                    string[] dateChunks = dataReader["dealDate"].ToString().Split('.');
                    string[] year = dateChunks[2].Split(' ');
                    result.dealDate = year[0]+"-"+dateChunks[1]+"-"+dateChunks[0];
                    result.dealNumber = dataReader["dealNumber"].ToString();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Не удалось получить данные из базы.");
                Console.WriteLine(ex);
            }
            finally
            {
                if (dataReader != null && !dataReader.IsClosed)
                {
                    dataReader.Close();
                }
            }
            return result;
        }

        private static string GetData(string url, Body body, int size, int number)
        {
            try
            {
                using(HttpClientHandler hdl = new HttpClientHandler
                {
                    AllowAutoRedirect = false, 
                    AutomaticDecompression = System.Net.DecompressionMethods.GZip | System.Net.DecompressionMethods.Deflate | System.Net.DecompressionMethods.None
                })
                {
                    using(HttpClient client = new HttpClient(hdl))
                    {
                        client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                        client.DefaultRequestHeaders.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.148 YaBrowser/22.7.2.899 Yowser/2.5 Safari/537.36");
                        client.DefaultRequestHeaders.Add("Connection", "keep-alive");
                        client.DefaultRequestHeaders.Add("Accept-Encoding", "gzip, deflate, br");
                        client.DefaultRequestHeaders.Add("Accept", "*/*");

                        body.variables.size = size;
                        body.variables.number = number;

                        var serializer = new JavaScriptSerializer();
                        var json = serializer.Serialize(body);

                        var stringContent = new StringContent(json, Encoding.UTF8, "application/json");

                        var postResponse = client.PostAsync(url, stringContent).Result;
                        postResponse.EnsureSuccessStatusCode();

                        return postResponse.Content.ReadAsStringAsync().Result;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Не удалось получить данные с сайта.");
                Console.WriteLine(ex.Message);
            }
            return null;
        }

        private static async Task InsertFirmAsync(Firm firm, SqlConnection sqlConnection)
        {
            SqlCommand sqlCommand = new SqlCommand(
                $"INSERT INTO [Firms] (sellerName, sellerInn, buyerName, buyerInn, woodVolumeBuyer, woodVolumeSeller, dealDate, dealNumber) VALUES (N'{firm.sellerName}', '{firm.sellerInn}', N'{firm.buyerName}', '{firm.buyerInn}', '{firm.woodVolumeBuyer}', '{firm.woodVolumeSeller}', '{firm.dealDate}', '{firm.dealNumber}')", 
                sqlConnection);
            await sqlCommand.ExecuteNonQueryAsync();
        }
    }
}


using System.Data.SqlClient;
using System.Xml;
using System.Xml.Linq;

class ETL
{
    static string stringConnection;
    static string inboxPath;
    static string XmlPath;
    string pathconfig = @"C:\code\ETL\ETL\";
    public void TestDB()
    {

        using (SqlConnection connection = new SqlConnection(stringConnection))
        {
            connection.Open();
            Console.WriteLine("Connected to SQL Server!");
            connection.Close();
            UpdateLog("Base de datos","Conexión exitosa");
        }
        ValidarRuta();
    }


    public void UpdateLog(String EventoTipo,String EventoInfo)
    {

        using (SqlConnection connection = new SqlConnection(stringConnection))
        {
            connection.Open();
            Console.WriteLine("Connected to SQL Server!");
            string insertSql = "INSERT INTO Eventos (EventTime, EventType, EventMessage) VALUES (@EventTime, @EventType, @EventMessage)";

            using (SqlCommand command = new SqlCommand(insertSql, connection))
            {
                command.Parameters.AddWithValue("@EventTime", DateTime.Now);
                command.Parameters.AddWithValue("@EventType", EventoTipo);
                command.Parameters.AddWithValue("@EventMessage", EventoInfo
                    );

                int rowsAffected = command.ExecuteNonQuery();

                Console.WriteLine("Log: Inserted {0} row(s)", rowsAffected);
            }
            connection.Close();
        }
        
    }
    public String ValidarRuta()
    {
        
        if (System.IO.Directory.Exists(inboxPath))
        {
            Console.WriteLine("El directorio existe");
            UpdateLog("Aviso", "Directorio inbox encontrado");
        }
        else
        {
            Console.WriteLine("El directorio no existe");
            UpdateLog("Aviso", "Directorio inbox no encontrado");
        }
        return "";
    }
   
    public void LeerConfig()
    {
        Dictionary<string, string> configValues = ReadConfigFile(pathconfig+"config.cfg");

      
        if (configValues.ContainsKey("InboxPath"))
        {
            inboxPath = configValues["InboxPath"];
        }

        
        if (configValues.ContainsKey("StringConnection"))
        {
            stringConnection = configValues["StringConnection"];
        }

        
        if (configValues.ContainsKey("XmlPath"))
        {
            XmlPath = configValues["XmlPath"];
        }

        Console.WriteLine("InboxPath: " + inboxPath);
        Console.WriteLine("StringConnection: " + stringConnection);
        Console.WriteLine("XmlPath: " + XmlPath);
    }

    private static Dictionary<string, string> ReadConfigFile(string fileName)
    {
        Dictionary<string, string> configValues = new Dictionary<string, string>();

        using (StreamReader reader = new StreamReader(fileName))
        {
            while (!reader.EndOfStream)
            {
                string line = reader.ReadLine();
                int separatorIndex = line.IndexOf('=');

                if (separatorIndex >= 0)
                {
                    string key = line.Substring(0, separatorIndex).Trim();
                    string value = line.Substring(separatorIndex + 1).Trim();

                    configValues[key] = value;
                }
            }
        }

        return configValues;

    }


    public void ReadXml()
    {
        XmlDocument xmlDoc = new XmlDocument();
        xmlDoc.Load(XmlPath);

        XmlNode headerNode = xmlDoc.SelectSingleNode("header");
        string asnNumber = headerNode.SelectSingleNode("asnNumber").InnerText;
        string documentReference = headerNode.SelectSingleNode("documentReference").InnerText;
        string comment2 = headerNode.SelectSingleNode("comment2").InnerText;
        Console.WriteLine("asnNumber: " + asnNumber);
        Console.WriteLine("documentReference: " + documentReference);
        Console.WriteLine("comment2: " + comment2);

        XmlNodeList lineItemNodes = xmlDoc.SelectNodes("header/lineItem");
        foreach (XmlNode lineItemNode in lineItemNodes)
        {
            string asnReferenceLine = lineItemNode.SelectSingleNode("asnReferenceLine").InnerText;
            string itemCode = lineItemNode.SelectSingleNode("itemCode").InnerText;
            string quantityOrdered = lineItemNode.SelectSingleNode("quantityOrdered").InnerText;
            string itemPrice = lineItemNode.SelectSingleNode("itemPrice").InnerText;

            Console.WriteLine("asnReferenceLine: " + asnReferenceLine);
            Console.WriteLine("itemCode: " + itemCode);
            Console.WriteLine("quantityOrdered: " + quantityOrdered);
            Console.WriteLine("itemPrice: " + itemPrice);
        }

    }
    


    public static void Main(string[] args)
    {
        ETL programa = new ETL();
        programa.LeerConfig();
        programa.TestDB();
        programa.ReadXml();
        
        
    }
}
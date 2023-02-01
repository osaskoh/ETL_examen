using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;

class ETL
{
    static string stringConnection;
    static string inboxPath;
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

                Console.WriteLine("Inserted {0} row(s)", rowsAffected);
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
        Dictionary<string, string> configValues = ReadConfigFile(pathconfig+"/config.cfg");

        Console.WriteLine("InboxPath: " + configValues["InboxPath"]);
        Console.WriteLine("StringConnection: " + configValues["StringConnection"]);
    }

    private static Dictionary<string, string> ReadConfigFile(string fileName)
    {
        Dictionary<string, string> configValues = new Dictionary<string, string>();

        try
        {
            using (StreamReader reader = new StreamReader(fileName))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (line.StartsWith("#"))
                    {
                        continue;
                    }

                    int index = line.IndexOf("=");
                    if (index < 0)
                    {
                        continue;
                    }

                    string key = line.Substring(0, index).Trim();
                    string value = line.Substring(index + 1).Trim();

                    configValues[key] = value;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error reading config file: " + ex.Message);
        }

        if (configValues.ContainsKey("InboxPath"))
        {
            inboxPath = configValues["InboxPath"];
        }

        if (configValues.ContainsKey("StringConnection"))
        {
            stringConnection = configValues["StringConnection"];
        }
        return configValues;

    }

    


    public static void Main(string[] args)
    {
        ETL programa = new ETL();
        programa.LeerConfig();
       // programa.TestDB();
        
        
    }
}
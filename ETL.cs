
using System.Data.SqlClient;
using System.Reflection.PortableExecutable;
using System.Xml;
using System.Xml.Linq;
class ETL
{

    //Variables del XML
    string asnNumber;
    string documentReference;
    string comment2;
    string asnReferenceLine;
    string itemCode;
    string quantityOrdered;
    string itemPrice;
    //Variables de entorno
    static string stringConnection;
    static string inboxPath;
    static string XmlPath;
    bool errorflag = false;
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
        UpdateLog("Aviso","Archivo de configuraciones cargado");
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
        UpdateLog("Aviso", "Archivo xml cargado");
        XmlNode headerNode = xmlDoc.SelectSingleNode("header");
        string asnNumber = headerNode.SelectSingleNode("asnNumber").InnerText;
        string documentReference = headerNode.SelectSingleNode("documentReference").InnerText;
        string comment2 = headerNode.SelectSingleNode("comment2").InnerText;
        Console.WriteLine("asnNumber: " + asnNumber);
        Console.WriteLine("documentReference: " + documentReference);
        Console.WriteLine("comment2: " + comment2);
        ValidarHeader();
        if (!errorflag)
        {
            InsertarHeader(asnNumber,documentReference,comment2);
        }

        XmlNodeList lineItemNodes = xmlDoc.SelectNodes("header/lineItem");
        foreach (XmlNode lineItemNode in lineItemNodes)
        {
            errorflag = false;
            string asnReferenceLine = lineItemNode.SelectSingleNode("asnReferenceLine").InnerText;
            string itemCode = lineItemNode.SelectSingleNode("itemCode").InnerText;
            string quantityOrdered = lineItemNode.SelectSingleNode("quantityOrdered").InnerText;
            string itemPrice = lineItemNode.SelectSingleNode("itemPrice").InnerText;
           

            Console.WriteLine("asnReferenceLine: " + asnReferenceLine);
            Console.WriteLine("itemCode: " + itemCode);
            Console.WriteLine("quantityOrdered: " + quantityOrdered);
            Console.WriteLine("itemPrice: " + itemPrice);
            // Validación para asnReferenceLine
            if (!int.TryParse(asnReferenceLine, out int asnReferenceLineValue))
            {
                Console.WriteLine("Error: asnReferenceLine no es un número entero valor: " + asnReferenceLine);
                errorflag = true;
                UpdateLog("error", "Error: asnReferenceLine no es un número entero valor: " + asnReferenceLine);
            }

            // Validación para itemCode
            if (itemCode?.Length > 15)
            {
                Console.WriteLine("Error: itemCode excede la longitud máxima de 15 caracteres");
                errorflag = true;
                UpdateLog("error", "Error: itemCode excede la longitud máxima de 15 caracteres");
            }

            // Validación para quantityOrdered
            if (!int.TryParse(quantityOrdered, out int quantityOrderedValue))
            {
                Console.WriteLine("Error: quantityOrdered no es un número entero valor: " + quantityOrdered);
                errorflag = true;
                UpdateLog("error", "Error: quantityOrdered no es un número entero valor: " + quantityOrdered);
            }

            // Validación para itemPrice
            if (!Decimal.TryParse(itemPrice, out decimal itemPriceValue))
            {
                Console.WriteLine("Error: itemPrice no es un número decimal valor: " + itemPrice);
                errorflag = true;
                UpdateLog("error", "Error: itemPrice no es un número decimal valor: " + itemPrice);
            }
            

            if (!errorflag)
            {
                InsertarDetail(asnNumber, Int32.Parse(asnReferenceLine), Int32.Parse(itemCode), Int32.Parse(quantityOrdered), double.Parse(itemPrice, System.Globalization.CultureInfo.InvariantCulture));
            }
        }

    }

    public void ValidarHeader()
    {
        errorflag = false;
        // Validación para asnNumber
        if (asnNumber?.Length > 20)
        {
            Console.WriteLine("Error: asnNumber excede la longitud máxima de 20 caracteres valor: " + asnNumber);
            errorflag = true;
        }

        // Validación para documentReference
        if (documentReference?.Length > 20)
        {
            Console.WriteLine("Error: documentReference excede la longitud máxima de 20 caracteres valor: " + documentReference);
            errorflag = true;
        }

        // Validación para comment2
        if (comment2?.Length > 20)
        {
            Console.WriteLine("Error: comment2 excede la longitud máxima de 20 caracteres valor: " + comment2);
            errorflag = true;
        }
    }

    public void InsertarHeader(string asnNumber, string documentReference, string comment2)
    {

        // Crea la conexión y abre
        using (SqlConnection connection = new SqlConnection(stringConnection))
        {
            connection.Open();

            // Crea la consulta SQL para insertar los datos
            string insertSql = "INSERT INTO Header (Referencia, ReferenciaDocumento, Direccion) " +
                               "VALUES (@asnNumber, @documentReference, @comment2)";

            // Crea el comando SQL
            using (SqlCommand insertCommand = new SqlCommand(insertSql, connection))
            {
                // Asigna los valores a los parámetros
                insertCommand.Parameters.AddWithValue("@asnNumber", asnNumber);
                insertCommand.Parameters.AddWithValue("@documentReference", documentReference);
                insertCommand.Parameters.AddWithValue("@comment2", comment2);

               
                try
                {
                    // Ejecuta la consulta
                    int rowsInserted = insertCommand.ExecuteNonQuery();
                    Console.WriteLine("Registros insertados: " + rowsInserted);
                    UpdateLog("Base de datos", "Registros ingresados a la tabla Header");
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    UpdateLog("Error", "No fue posible insertar registro en tabla Header, error: " + e);
                }
            }
        }
    }

    public void InsertarDetail(String Referencia, int ReferenciaLine, int Itemcode, int Cantidad, double Precio)
    {
        using (SqlConnection connection = new SqlConnection(stringConnection))
        {
            // Abrir la conexión
            connection.Open();

            // Crear un comando SQL para insertar los valores
            using (SqlCommand command = new SqlCommand("INSERT INTO Detail (Referencia, LineaReferencia, itemcode, Cantidad, Precio) VALUES (@Referencia, @LineaReferencia, @itemcode, @Cantidad,CONVERT(Numeric(5,3),@Precio) )", connection))
            {
                // Agregar los valores como parámetros para prevenir la inyección de SQL
                command.Parameters.AddWithValue("@Referencia", Referencia);
                command.Parameters.AddWithValue("@LineaReferencia", ReferenciaLine);
                command.Parameters.AddWithValue("@itemcode", Itemcode);
                command.Parameters.AddWithValue("@Cantidad", Cantidad);
                command.Parameters.AddWithValue("@Precio", Precio);
                try
                {
                    // Ejecutar el comando SQL
                    int rowsAffected = command.ExecuteNonQuery();
                    Console.WriteLine("Registros insertados: " + rowsAffected);
                    UpdateLog("Base de datos", "Registros ingresados a la tabla Detail");
                }catch(Exception e)
                {
                    Console.WriteLine(e.Message);
                    UpdateLog("Error","No fue posible insertar registro en tabla Detail, error: "+e);
                }
                
                
            }
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
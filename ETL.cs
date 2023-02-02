
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
    static string stringConnection; //variable que almacena la cadena de conexión a la BD
    static string inboxPath; //Variavle que almacena la ruta de la carpeta inbox
    static string XmlPath; //variable que almacena la ruta del archivo XML y su nombre.
    bool errorflag = false; //variable auxiliar para detectar errores de validación en tiempo real
    string pathconfig = @"C:\code\ETL\ETL\";
    public void TestDB()
    {
        //valida que se pueda conectar a la base de datos con los parametros de conexión extraidos del archivo config
        using (SqlConnection connection = new SqlConnection(stringConnection))
        {
            connection.Open();
            Console.WriteLine("Connected to SQL Server!");
            connection.Close();
            //crea log de el evento, a partir de aquí se hará en cada evento crucial en el proceso etl.
            UpdateLog("Base de datos","Conexión exitosa");
        }
        ValidarRuta();
    }

    //método para ingresar un registro a la tabla log , donde recibe dos parametros y algunos más se calculan en la inserción "datetime" por ejemplo
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
    //Método para validar que exista la carpeta inbox en el path que se le dió en el archivo config
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
   //Método para leer el archivo config y asignar el contenido en variables de entorno
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
    //método para crear un diccionario de variables, en donde el Key va a la izquierda limitado por un signo "="
    //y a la derecha el value, este método sirve se adapta para ingresar más variables en el futuro
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

    //Método para leer el archivo xml, el path y el nombre del archivo se extrae del archivo config
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
        //en el siguiente foreach evaluamos si existen más de 1 combo de registros "item" en el archivo
        //por cada ciclo foreach se evaluan los datos y se ingresan paralelamente a la base de datos en la tabla Detail
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
    //Método para validar las reglas de variables en los campos de Header
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
    //Método para insertar los valores a la table header una vez validados
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
    //Método para insertar valores en la tabla detail una vez validados
    public void InsertarDetail(String Referencia, int ReferenciaLine, int Itemcode, int Cantidad, double Precio)
    {
        using (SqlConnection connection = new SqlConnection(stringConnection))
        {
            // Abrir la conexión
            connection.Open();

            // Crear un comando SQL para insertar los valores
            using (SqlCommand command = new SqlCommand("INSERT INTO Detail (Referencia, LineaReferencia, itemcode, Cantidad, Precio) VALUES (@Referencia, @LineaReferencia, @itemcode, @Cantidad,@Precio)", connection))
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
    /*
    public void AjustarPrecio(decimal itemPrice)
    {
        
        decimal adjustedPrice = AdjustPrice(itemPrice);
        Console.WriteLine("Precio antes del ajuste: " + itemPrice);
        Console.WriteLine("Precio despues del ajuste: " + adjustedPrice);
    }

    static decimal AdjustPrice(decimal price)
    {
        int precision = price.ToString().Split('.')[0].Length;
        if (precision > 2)
        {
            price = Math.Round(price, 2 - precision, MidpointRounding.AwayFromZero);
        }
        string priceString = price.ToString("F3");
        return decimal.Parse(priceString);
    }
    */

    public static void Main(string[] args)
    {
        ETL programa = new ETL();
        programa.LeerConfig();
        programa.TestDB();
        programa.ReadXml();
        
        
        
    }
}
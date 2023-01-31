class ETL
{
    public String ValidarRuta()
    {
        string path = @"C:\code\ETL\ETL\inbox";
        if (System.IO.Directory.Exists(path))
        {
            Console.WriteLine("El directorio existe");
        }
        else
        {
            Console.WriteLine("El directorio no existe");
        }
        return "";
    }



    public static void Main(string[] args)
    {
        ETL programa = new ETL();
        programa.ValidarRuta();
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Data.OleDb;
using System.Data;
using System.Data.SqlClient;
using IBM.Data.DB2.iSeries;
using System.Collections;
namespace DA
{
    public class DA_Ordenes
    {
        string ConexionAS400 = ConfigurationManager.ConnectionStrings["ConexionAS400"].ConnectionString;
        string ConexionSQL = ConfigurationManager.ConnectionStrings["ConexionSQL"].ConnectionString;


        public DataTable ObtenerDatosOrdenes(int CODCIA, string CODTDC, int ANOOR1, int NROOR1)
        {
            DataTable tabla = new DataTable();
            using (iDB2Connection  con = new iDB2Connection(ConexionAS400))
            {
                con.Open();
                string ConsultaDB2 = "SELECT FCMPORD1.CODCIA, FCMPORD1.CODTDC, FCMPORD1.ANOOR1, FCMPORD1.NROOR1, FCMPORD1.CODCPP, TABCONPP.DESCPP, " +
                                     "TABCONPP.STACPP, FCMPORD1.CODPRV, FABPROVE.RSRPRV, FABPROVE.DIRPRV, FABPROVE.TE1PRV, FABPROVE.RUCPRV " +
                                     "FROM (FCMPORD1 INNER JOIN TABCONPP ON FCMPORD1.CODCPP = TABCONPP.CODCPP) INNER JOIN FABPROVE ON FCMPORD1.CODPRV = FABPROVE.CODPRV " +
                                     "WHERE FCMPORD1.CODCIA=? AND FCMPORD1.CODTDC=? AND FCMPORD1.ANOOR1=? AND FCMPORD1.NROOR1=?";

                using (iDB2Command cmd = new iDB2Command(ConsultaDB2, con))
                {
                    cmd.Parameters.AddWithValue("@CODCIA", CODCIA);
                    cmd.Parameters.AddWithValue("@CODTDC", CODTDC);
                    cmd.Parameters.AddWithValue("@ANOOR1", ANOOR1);
                    cmd.Parameters.AddWithValue("@NROOR1", NROOR1);

                    iDB2DataAdapter oda = new iDB2DataAdapter(cmd);
                    oda.Fill(tabla);
                }
            }
            return tabla;
        }

        public DataTable ObtenerOrdenes()
        {
            string log = string.Empty;
            DataTable table = new DataTable();
            using (SqlConnection con = new SqlConnection(ConexionSQL))
            {
                con.Open();
              

                using (SqlCommand cmd = new SqlCommand("SP_OBTENER_ORDENES", con))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    SqlDataAdapter oda = new SqlDataAdapter(cmd);
                    oda.Fill(table);

                       
                }
            }
            return table;
        }
        public string ActualizaDependencias(int CODCIA, string CODTDC, int ANOOR1, int NROOR1)
        {
            string log = string.Empty;
            using (SqlConnection con = new SqlConnection(ConexionSQL))
            {
                con.Open();

                using (SqlCommand cmd = new SqlCommand("SP_ACTUALIZAR_DEPENDENCIAS", con))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@CODCIA", CODCIA);
                    cmd.Parameters.AddWithValue("@CODTDC", CODTDC);
                    cmd.Parameters.AddWithValue("@ANOOR1", ANOOR1);
                    cmd.Parameters.AddWithValue("@NROOR1", NROOR1);
                    cmd.Parameters.Add("@Log", SqlDbType.VarChar, -1).Direction = ParameterDirection.Output;

                    cmd.ExecuteNonQuery();
                    log = cmd.Parameters["@Log"].Value.ToString();
                }
            }
            return log;
        }
        public DataTable ObtenerDatosCompañias()
        {
            DataTable tabla = new DataTable();
            using (iDB2Connection con = new iDB2Connection(ConexionAS400))
            {
                con.Open();
                string ConsultaDB2 = "SELECT * FROM TABCONTR WHERE CODTCO = 257";
              

                using (iDB2Command cmd = new iDB2Command(ConsultaDB2, con))
                {

                    iDB2DataAdapter oda = new iDB2DataAdapter(cmd);
                    oda.Fill(tabla);
                }
            }
            return tabla;
        }
        public DataTable ObtenerDatosAprobNoDet(int CODCIA)
        {
            DataTable tabla = new DataTable();
            using (iDB2Connection con = new iDB2Connection(ConexionAS400))
            {
                con.Open();
                string ConsultaDB2 = "SELECT ANDCIA FROM MAESCIAS WHERE CODCIA =" + CODCIA;


                using (iDB2Command cmd = new iDB2Command(ConsultaDB2, con))
                {

                    iDB2DataAdapter oda = new iDB2DataAdapter(cmd);
                    oda.Fill(tabla);
                }
            }
            return tabla;
        }
        public DataTable ObtenerDatosAprobOrdenA(int CODCIA)
        {
            DataTable tabla = new DataTable();
            using (iDB2Connection con = new iDB2Connection(ConexionAS400))
            {
                con.Open();
                string ConsultaDB2 = "SELECT * FROM FCMCIAAP WHERE CODTCO = 189 AND CODCIA = " + CODCIA;


                using (iDB2Command cmd = new iDB2Command(ConsultaDB2, con))
                {

                    iDB2DataAdapter oda = new iDB2DataAdapter(cmd);
                    oda.Fill(tabla);
                }
            }
            return tabla;
        }
        public DataTable ObtenerDatosAprobOrdenB(int CODCIA)
        {
            DataTable tabla = new DataTable();
            using (iDB2Connection con = new iDB2Connection(ConexionAS400))
            {
                con.Open();
                string ConsultaDB2 = "SELECT * FROM FCMCIAAP WHERE CODTCO = 190 AND CODCIA = " + CODCIA;


                using (iDB2Command cmd = new iDB2Command(ConsultaDB2, con))
                {

                    iDB2DataAdapter oda = new iDB2DataAdapter(cmd);
                    oda.Fill(tabla);
                }
            }
            return tabla;
        }
        public DataTable ObtenerDatosAprobOrdenC(int CODCIA)
        {
            DataTable tabla = new DataTable();
            using (iDB2Connection con = new iDB2Connection(ConexionAS400))
            {
                con.Open();
                string ConsultaDB2 = "SELECT * FROM FCMCIAAP WHERE CODTCO = 362 AND CODCIA = " + CODCIA;


                using (iDB2Command cmd = new iDB2Command(ConsultaDB2, con))
                {

                    iDB2DataAdapter oda = new iDB2DataAdapter(cmd);
                    oda.Fill(tabla);
                }
            }
            return tabla;
        }
        public DataTable ObtenerDatosTE_AprOrdenCabecera(string Query)
        {
            DataTable tabla = new DataTable();
            using (iDB2Connection con = new iDB2Connection(ConexionAS400))
            {
                con.Open();
                string ConsultaDB2 = Query;


                using (iDB2Command cmd = new iDB2Command(ConsultaDB2, con))
                {

                    iDB2DataAdapter oda = new iDB2DataAdapter(cmd);
                    oda.Fill(tabla);
                }
            }
            return tabla;
        }

        public string Insertar_TE_AprOrdenCabecera(int CODCIA2,string CODTDC2,int ANOOR12,string DCTO,DateTime FEMOR1,string CODPRV,string RSRPRV,string CODCPP,string CIFOR1,string IBROR1,string IDEOR1,string IGFOR1,string FLEOR1,string SEGOR1,string IIGOR1,string MPIOR1,string TDAOR1,string IESOR1,string RT4OR1,string RT2OR1,string AJPOR1,string AJNOR1,string IGEOR1,string CODMON,string CODCOM,string OBSOR1,string FPROR4,string TCAOR1,string COMPRADOR, string MaxDeFA, string MaxDeFB, string MaxDeFC,DateTime FEAOR1,int PPUOR1, string NAMFIL, string URLFIL,string QueryEliminar)
        {
            string log = string.Empty;
            try
            {
                
            using (SqlConnection con = new SqlConnection(ConexionSQL))
            {
                con.Open();

                // Antes de ejecutar el procedimiento almacenado, eliminamos los registros según el QueryEliminar
                using (SqlCommand cmdEliminar = new SqlCommand(QueryEliminar, con))
                {
                    cmdEliminar.ExecuteNonQuery();
                }

                // Luego, ejecutamos el procedimiento almacenado para insertar los nuevos datos
                using (SqlCommand cmdInsertar = new SqlCommand("SP_INSERT_TE_APRORDENCABECERA", con))
                {
                    cmdInsertar.CommandType = CommandType.StoredProcedure;

                        // SqlParameter parameter = new SqlParameter("@Data", SqlDbType.Structured);
                        //parameter.Value = xmlData;
                        // parameter.TypeName = "dbo.TE_APRORDENCABECERA";

                        cmdInsertar.Parameters.AddWithValue("@CODCIA", CODCIA2);
                        cmdInsertar.Parameters.AddWithValue("@CODTDC", CODTDC2);
                        cmdInsertar.Parameters.AddWithValue("@ANOOR1", ANOOR12);
                        cmdInsertar.Parameters.AddWithValue("@DCTO", DCTO);
                        cmdInsertar.Parameters.AddWithValue("@FEMOR1", FEMOR1);
                        cmdInsertar.Parameters.AddWithValue("@CODPRV", CODPRV);
                        cmdInsertar.Parameters.AddWithValue("@RSRPRV", RSRPRV);
                        cmdInsertar.Parameters.AddWithValue("@CODCPP", CODCPP);
                        cmdInsertar.Parameters.AddWithValue("@CIFOR1", CIFOR1);
                        cmdInsertar.Parameters.AddWithValue("@IBROR1", IBROR1);
                        cmdInsertar.Parameters.AddWithValue("@IDEOR1", IDEOR1);
                        cmdInsertar.Parameters.AddWithValue("@IGFOR1", IGFOR1);
                        cmdInsertar.Parameters.AddWithValue("@FLEOR1", FLEOR1);
                        cmdInsertar.Parameters.AddWithValue("@SEGOR1", SEGOR1);
                        cmdInsertar.Parameters.AddWithValue("@IIGOR1", IIGOR1);
                        cmdInsertar.Parameters.AddWithValue("@MPIOR1", MPIOR1);
                        cmdInsertar.Parameters.AddWithValue("@TDAOR1", TDAOR1);
                        cmdInsertar.Parameters.AddWithValue("@IESOR1", IESOR1);
                        cmdInsertar.Parameters.AddWithValue("@RT4OR1", RT4OR1);
                        cmdInsertar.Parameters.AddWithValue("@RT2OR1", RT2OR1);
                        cmdInsertar.Parameters.AddWithValue("@AJPOR1", AJPOR1);
                        cmdInsertar.Parameters.AddWithValue("@AJNOR1", AJNOR1);
                        cmdInsertar.Parameters.AddWithValue("@IGEOR1", IGEOR1);
                        cmdInsertar.Parameters.AddWithValue("@CODMON", CODMON);
                        cmdInsertar.Parameters.AddWithValue("@CODCOM", CODCOM);
                        cmdInsertar.Parameters.AddWithValue("@OBSOR1", OBSOR1);
                        cmdInsertar.Parameters.AddWithValue("@FPROR4", FPROR4);
                        cmdInsertar.Parameters.AddWithValue("@TCAOR1", TCAOR1);
                        cmdInsertar.Parameters.AddWithValue("@COMPRADOR", COMPRADOR);
                        cmdInsertar.Parameters.AddWithValue("@MaxDeFA", MaxDeFA);
                        cmdInsertar.Parameters.AddWithValue("@MaxDeFB", MaxDeFB);
                        cmdInsertar.Parameters.AddWithValue("@MaxDeFC", MaxDeFC);
                        cmdInsertar.Parameters.AddWithValue("@FEAOR1", FEAOR1);
                        cmdInsertar.Parameters.AddWithValue("@PPUOR1", PPUOR1);
                        cmdInsertar.Parameters.AddWithValue("@NAMFIL", NAMFIL);
                        cmdInsertar.Parameters.AddWithValue("@URLFIL", URLFIL);
                    


                    cmdInsertar.Parameters.Add("@Log", SqlDbType.VarChar, -1).Direction = ParameterDirection.Output;
                    cmdInsertar.ExecuteNonQuery();
                    log = cmdInsertar.Parameters["@Log"].Value.ToString();
                }
            }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
            return log;

        }


        public DataTable ObtenerDatosTE_AprOrdenDetalleOC(string Query)
        {
            DataTable tabla = new DataTable();
            using (iDB2Connection con = new iDB2Connection(ConexionAS400))
            {
                con.Open();
                string ConsultaDB2 = Query;


                using (iDB2Command cmd = new iDB2Command(ConsultaDB2, con))
                {

                    iDB2DataAdapter oda = new iDB2DataAdapter(cmd);
                    oda.Fill(tabla);
                }
            }
            return tabla;
        }
        public string Insertar_TE_AprOrdenDetalleOC(DataTable tabla, string QueryEliminar)
        {
            string log = string.Empty;
            using (SqlConnection con = new SqlConnection(ConexionSQL))
            {
                con.Open();

                // Antes de ejecutar el procedimiento almacenado, eliminamos los registros según el QueryEliminar
                using (SqlCommand cmdEliminar = new SqlCommand(QueryEliminar, con))
                {
                    cmdEliminar.ExecuteNonQuery();
                }

                // Luego, ejecutamos el procedimiento almacenado para insertar los nuevos datos
                using (SqlCommand cmdInsertar = new SqlCommand("SP_INSERT_TE_APRORDENDETALLEOC", con))
                {
                    cmdInsertar.CommandType = CommandType.StoredProcedure;

                    SqlParameter parameter = new SqlParameter("@Data", SqlDbType.Structured);
                    parameter.Value = tabla;
                    parameter.TypeName = "dbo.TE_AprOrdenDetalleOC";
                    cmdInsertar.Parameters.Add(parameter);
                    cmdInsertar.Parameters.Add("@Log", SqlDbType.VarChar, -1).Direction = ParameterDirection.Output;
                    cmdInsertar.ExecuteNonQuery();
                    log = cmdInsertar.Parameters["@Log"].Value.ToString();
                }
            }
            return log;
        }
        public DataTable ObtenerDatosTE_AprOrdenDetalleOD(string Query)
        {
            DataTable tabla = new DataTable();
            using (iDB2Connection con = new iDB2Connection(ConexionAS400))
            {
                con.Open();
                string ConsultaDB2 = Query;


                using (iDB2Command cmd = new iDB2Command(ConsultaDB2, con))
                {

                    iDB2DataAdapter oda = new iDB2DataAdapter(cmd);
                    oda.Fill(tabla);
                }
            }
            return tabla;
        }

        public string Insertar_TE_AprOrdenDetalleOD(int CODCIA2, string CODTDC2, int ANOOR12, string NROOR12, string CSCOR3, string CODTSV, string CENCOS, string DE1OR3,decimal IMPOR3, string SOLICITANTE, string QueryEliminar)
        {
            string log = string.Empty;
            try
            {
                using (SqlConnection con = new SqlConnection(ConexionSQL))
              {
                con.Open();

                // Antes de ejecutar el procedimiento almacenado, eliminamos los registros según el QueryEliminar
              /*  using (SqlCommand cmdEliminar = new SqlCommand(QueryEliminar, con))
                {
                    cmdEliminar.ExecuteNonQuery();
                }*/

                // Luego, ejecutamos el procedimiento almacenado para insertar los nuevos datos
                using (SqlCommand cmdInsertar = new SqlCommand("SP_INSERT_TE_APRORDENDETALLEOD", con))
                {
                    cmdInsertar.CommandType = CommandType.StoredProcedure;
                    cmdInsertar.Parameters.AddWithValue("@CODCIA", CODCIA2);
                    cmdInsertar.Parameters.AddWithValue("@CODTDC", CODTDC2);
                    cmdInsertar.Parameters.AddWithValue("@ANOOR1", ANOOR12);
                    cmdInsertar.Parameters.AddWithValue("@NROOR1", NROOR12);
                    cmdInsertar.Parameters.AddWithValue("@CSCOR3", CSCOR3);
                    cmdInsertar.Parameters.AddWithValue("@CODTSV", CODTSV);
                    cmdInsertar.Parameters.AddWithValue("@CENCOS", CENCOS);
                    cmdInsertar.Parameters.AddWithValue("@DE1OR3", DE1OR3);
                    cmdInsertar.Parameters.AddWithValue("@IMPOR3", IMPOR3);
                    cmdInsertar.Parameters.AddWithValue("@SOLICITANTE", SOLICITANTE);
                


                        cmdInsertar.Parameters.Add("@Log", SqlDbType.VarChar, -1).Direction = ParameterDirection.Output;
                    cmdInsertar.ExecuteNonQuery();
                    log = cmdInsertar.Parameters["@Log"].Value.ToString();
                }
              }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
            return log;
        }

        public DataTable ObtenerDatos_T_OperaAprOrden(string Query)
        {
          DataTable tabla = new DataTable();
              using ( iDB2Connection  con = new iDB2Connection(ConexionAS400))
            {
                con.Open();
                string ConsultaDB2 = Query;


                using (iDB2Command cmd = new iDB2Command(ConsultaDB2, con))
                {

                    iDB2DataAdapter oda = new iDB2DataAdapter(cmd);
                    oda.Fill(tabla);
                    
                }
            }
            return tabla;
        }
        public string Insertar_T_OperaAprOrden(int CODCIA,string CODTDC, string ANOOR1, string NROOR1, string SECOR4, string FMOOR4,
            string FPROR4, string OUROR4, string FVBOR4, string UVBOR4,
              string FA1OR4, string UA1OR4, string FD1OR4, string OD1OR4, string SA1OR4, string FA2OR4, string UA2OR4, string FD2OR4,
              string OD2OR4, string SA2OR4, string FA3OR4, string UA3OR4,
             string FD3OR4, string OD3OR4, string SA3OR4,string FHROR4, string STAOR4, string QueryEliminar)
        {
            string log = string.Empty;
            try
            {
                using (SqlConnection con = new SqlConnection(ConexionSQL))
                {
                con.Open();
                using (SqlCommand cmdEliminar = new SqlCommand(QueryEliminar, con))
                {
                    cmdEliminar.ExecuteNonQuery();
                }
                    using (SqlCommand cmdInsertar = new SqlCommand("SP_INSERT_T_OPERAAPRORDEN", con))
                    {
                    cmdInsertar.CommandType = CommandType.StoredProcedure;
                        cmdInsertar.Parameters.AddWithValue("@CODCIA", CODCIA);
                        cmdInsertar.Parameters.AddWithValue("@CODTDC", CODTDC);
                        cmdInsertar.Parameters.AddWithValue("@ANOOR1", ANOOR1);
                        cmdInsertar.Parameters.AddWithValue("@NROOR1", NROOR1);
                        cmdInsertar.Parameters.AddWithValue("@SECOR4", SECOR4);
                        cmdInsertar.Parameters.AddWithValue("@FMOOR4", FMOOR4);
                        cmdInsertar.Parameters.AddWithValue("@FPROR4", FPROR4);
                        cmdInsertar.Parameters.AddWithValue("@OUROR4", OUROR4);
                        cmdInsertar.Parameters.AddWithValue("@FVBOR4", FVBOR4);
                        cmdInsertar.Parameters.AddWithValue("@UVBOR4", UVBOR4);
                        cmdInsertar.Parameters.AddWithValue("@FA1OR4", FA1OR4);
                        cmdInsertar.Parameters.AddWithValue("@UA1OR4", UA1OR4);
                        cmdInsertar.Parameters.AddWithValue("@FD1OR4", FD1OR4);
                        cmdInsertar.Parameters.AddWithValue("@OD1OR4", OD1OR4);
                        cmdInsertar.Parameters.AddWithValue("@SA1OR4", SA1OR4);
                        cmdInsertar.Parameters.AddWithValue("@FA2OR4", FA2OR4);
                        cmdInsertar.Parameters.AddWithValue("@UA2OR4", UA2OR4);
                        cmdInsertar.Parameters.AddWithValue("@FD2OR4", FD2OR4);
                        cmdInsertar.Parameters.AddWithValue("@OD2OR4", OD2OR4);
                        cmdInsertar.Parameters.AddWithValue("@SA2OR4", SA2OR4);
                        cmdInsertar.Parameters.AddWithValue("@FA3OR4", FA3OR4);
                        cmdInsertar.Parameters.AddWithValue("@UA3OR4", UA3OR4);
                        cmdInsertar.Parameters.AddWithValue("@FD3OR4", FD3OR4);
                        cmdInsertar.Parameters.AddWithValue("@OD3OR4", OD3OR4);
                        cmdInsertar.Parameters.AddWithValue("@SA3OR4", SA3OR4);
                        cmdInsertar.Parameters.AddWithValue("@FHROR4", FHROR4);
                        cmdInsertar.Parameters.AddWithValue("@STAOR4", STAOR4);
                      



                        cmdInsertar.Parameters.Add("@Log", SqlDbType.VarChar, -1).Direction = ParameterDirection.Output;
                    cmdInsertar.ExecuteNonQuery();
                    log = cmdInsertar.Parameters["@Log"].Value.ToString();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
            return log;
        }

        public string Eliminar_Datos_Tablas(int CODCIA, string CODTDC, int ANOOR1, int NROOR1)
        {
            string log = string.Empty;
            try
            {
                using (SqlConnection con = new SqlConnection(ConexionSQL))
                {
                    con.Open();

                    using (SqlCommand cmd = new SqlCommand("SP_ELIMINAR_DATOS_TABLAS", con))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.AddWithValue("@CODCIA", CODCIA);
                        cmd.Parameters.AddWithValue("@CODTDC", CODTDC);
                        cmd.Parameters.AddWithValue("@ANOOR1", ANOOR1);
                        cmd.Parameters.AddWithValue("@NROOR1", NROOR1);
                        cmd.Parameters.Add("@Log", SqlDbType.VarChar, -1).Direction = ParameterDirection.Output;
                        cmd.ExecuteNonQuery();
                        log = cmd.Parameters["@Log"].Value.ToString();
                    }

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
            return log;
        }

        public void CambiaEstadoOrden(string Query)
        {
           
          
            using (iDB2Connection con = new iDB2Connection(ConexionAS400))
            {
                con.Open();
                string ConsultaDB2 = Query;


                using (iDB2Command cmd = new iDB2Command(ConsultaDB2, con))
                {
                    cmd.ExecuteNonQuery();
                }
            }
           
        }
        
    }
}

using DA;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.OleDb;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Data.SqlClient;
using static System.Windows.Forms.VisualStyles.VisualStyleElement;

namespace UI
{
    public partial class Ordenes : ServiceBase
    {
        DA_Ordenes obj_DA = new DA_Ordenes();

        bool Bandera = false;
        public Ordenes()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {

        }

        protected override void OnStop()
        {
        }

        private void timer1_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {


            if (Bandera) return;
            try
            {
                Bandera = true;
                ABCOrdenesWeb();
              
        
            }
            catch (Exception)
            {

                throw;
            }
            Bandera = false;

        }

        public void ABCOrdenesWeb()
        {
            int CODCIA; string CODTDC; int ANOOR1; int NROOR1; int Op; int OPAPR;
            DataTable orden=new DataTable();
           


            try
            {
                orden = obj_DA.ObtenerOrdenes();
                if (orden.Rows.Count > 0)
                {
                    CODCIA = Convert.ToInt32(orden.Rows[0][0].ToString());
                    CODTDC = orden.Rows[0][1].ToString();
                    ANOOR1 = Convert.ToInt32(orden.Rows[0][2].ToString());
                    NROOR1 = Convert.ToInt32(orden.Rows[0][3].ToString());

                    ActualizaDependencias(CODCIA, CODTDC, ANOOR1, NROOR1);
                    ActualizaOrdenWeb(CODCIA, CODTDC, ANOOR1, NROOR1);
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private void ActualizaDependencias(int CODCIA, string CODTDC, int ANOOR1, int NROOR1)
        {
            string Log = "";
            DataTable dt = new DataTable();
            dt = obj_DA.ObtenerDatosOrdenes(CODCIA, CODTDC, ANOOR1, NROOR1);

            if (dt.Rows.Count > 0)
            {

                Log = obj_DA.ActualizaDependencias(CODCIA, CODTDC, ANOOR1, NROOR1);
            }


        }

        private void ActualizaOrdenWeb(int CODCIA, string CODTDC, int ANOOR1, int NROOR1)
        {
            try
            {
               string log1= Eliminar_Datos_Tablas(CODCIA, CODTDC, ANOOR1, NROOR1);
               string log2 = TE_AprOrdenCabecera(CODCIA, CODTDC, ANOOR1, NROOR1);
               string log3 = TE_AprOrdenDetalleOD(CODCIA, CODTDC, ANOOR1, NROOR1);
               string log4 = T_OperaAprOrden(CODCIA, CODTDC, ANOOR1, NROOR1);
               CambiaEstadoOrden(CODCIA, CODTDC, ANOOR1, NROOR1);
                // Ruta del archivo de texto
                string filePath = @"D:\LogServicioOrdenesWeb\log.txt";

                // Escribir los logs en el archivo
                using (StreamWriter sw = File.AppendText(filePath))
                {
                    LogToTxt(sw, "********************* INICIO *********************");
                    LogToTxt(sw, "CODCIA :" + CODCIA + "| CODTDC : "+ CODTDC + "| ANOOR1 : " + ANOOR1 + "| NROOR1 : " + NROOR1);
                    LogToTxt(sw, log1);
                    LogToTxt(sw, log2);
                    LogToTxt(sw, log3);
                    LogToTxt(sw, log4);
                    LogToTxt(sw, "********************* FIN *********************");
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }
        static void LogToTxt(StreamWriter sw, string log)
        {
            sw.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {log}");
        }
        public string Eliminar_Datos_Tablas(int CODCIA, string CODTDC, int ANOOR1, int NROOR1)
        {

            return obj_DA.Eliminar_Datos_Tablas(CODCIA, CODTDC, ANOOR1,NROOR1);

        }

        public string TE_AprOrdenCabecera(int CODCIA, string CODTDC, int ANOOR1, int NROOR1)
        {
            // Variable declarations
            string VQuery1 = "";
            string VQuery2 = "";
            string VQuery3 = "";
            string VAPNDET = "";
            string CIA2APR = "N";
           
            DataTable tabla = new DataTable();
            tabla = obj_DA.ObtenerDatosCompañias();

            if (tabla.Rows.Count==0)
            {
                CIA2APR = "N";
            }
            else
            {
                CIA2APR = "N";
                for (int i = 0; i < 9; i++)
                {
                    try
                    {
                      
                        string subString = tabla.Rows[0][3].ToString().Substring(i, 2); // Handle potential null value in TEXTCO

                        if (subString != null && CODCIA.ToString() == subString)
                        {
                            CIA2APR = "S";
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        // Handle potential exceptions during conversion or substring extraction (optional)
                        Console.WriteLine($"Error during iteration: {ex.Message}");
                    }
                }
            }


            //  Determina codigo e Aprobacion de Ordenes no determinantes
            tabla = obj_DA.ObtenerDatosAprobNoDet(CODCIA);


            if (tabla.Rows.Count == 0)
            {
                VAPNDET = "0";
            }
            else
            {
               
                VAPNDET = tabla.Rows[0][0].ToString();
            }


            // Para aprobación 1
            //cias autorizadas aprobación A
            tabla = obj_DA.ObtenerDatosAprobOrdenA(CODCIA);


            if (tabla.Rows.Count == 0)
            {
                if (VAPNDET == "1")
                {
                    VQuery1 = "'S'";
                }
                else
                {
                    if (CIA2APR == "S")
                    {
                        VQuery1 = "'S'";
                    }
                    else
                    {
                        VQuery1 = "NULL";
                    }
                }
            }
            else
            {
                if (VAPNDET == "1")
                {
                    VQuery1 = "'S'";
                }
                else
                {
                    if (CIA2APR == "S")
                    {
                        VQuery1 = "'S'";
                    }
                    else
                    {
                        VQuery1 = "'S'";
                    }
                }
            }
            //Para aprobación 2
            //cias autorizadas aprobación B
            tabla = obj_DA.ObtenerDatosAprobOrdenB(CODCIA);
       
            if (tabla.Rows.Count == 0)
            {
                if (VAPNDET == "2")
                {
                    VQuery2 = "'S'";
                }
                else
                {
                    if (CIA2APR == "S")
                    {
                        VQuery2 = "'S'";
                    }
                    else
                    {
                        VQuery2 = "NULL";
                    }
                }
            }
            else
            {
                if (VAPNDET == "2")
                {
                    VQuery2 = "'S'";
                }
                else
                {
                    if (CIA2APR == "S")
                    {
                        VQuery2 = "'S'";
                    }
                    else
                    {
                        VQuery2 = "'S'";
                    }
                }
            }

            //Para aprobación 3
            //cias autorizadas aprobación C
            tabla = obj_DA.ObtenerDatosAprobOrdenC(CODCIA);
           
            if (tabla.Rows.Count == 0)
            {
                if (VAPNDET == "3")
                {
                    VQuery3 = "'S'";
                }
                else
                {
                    if (CIA2APR == "S")
                    {
                        VQuery3 = "'S'";
                    }
                    else
                    {
                        VQuery3 = "NULL";
                    }
                }
            }
            else
            {
                if (VAPNDET == "3")
                {
                    VQuery3 = "'S'";
                }
                else
                {
                    if (CIA2APR == "S")
                    {
                        VQuery3 = "'S'";
                    }
                    else
                    {
                        VQuery3 = "'S'";
                    }
                }
            }

            VQuery1 = @"CASE WHEN FCMPORD4.ANDOR4 = 0 THEN case when FCMCIAAP.codtco=189 then 'S' when FCMCIAAP.codtco=190 then null when FCMCIAAP.codtco=362 then null when FCMCIAAP.codtco=257 then 'S' else 'S' end " +
                    " WHEN FCMPORD4.ANDOR4 = 1 THEN case when FCMCIAAP.codtco=189 then 'S' when FCMCIAAP.codtco=190 then null when FCMCIAAP.codtco=362 then null when FCMCIAAP.codtco=257 then 'S' else 'S' end " +
                    " WHEN FCMPORD4.ANDOR4 = 2 THEN case when FCMCIAAP.codtco=189 then 'S' when FCMCIAAP.codtco=190 then null when FCMCIAAP.codtco=362 then null when FCMCIAAP.codtco=257 then 'S' else 'S' end " +
                    " WHEN FCMPORD4.ANDOR4 = 3 THEN case when FCMCIAAP.codtco=189 then 'S' when FCMCIAAP.codtco=190 then null when FCMCIAAP.codtco=362 then null when FCMCIAAP.codtco=257 then 'S' else 'S' end " +
                    " End ";
            VQuery2 = @"CASE WHEN FCMPORD4.ANDOR4 = 0 THEN case when FCMCIAAP.codtco=189 then null when FCMCIAAP.codtco=190 then 'S' when FCMCIAAP.codtco=362 then null when FCMCIAAP.codtco=257 then 'S' else 'S' end " +
                    " WHEN FCMPORD4.ANDOR4 = 1 THEN case when FCMCIAAP.codtco=189 then null when FCMCIAAP.codtco=190 then 'S' when FCMCIAAP.codtco=362 then null when FCMCIAAP.codtco=257 then 'S' else 'S' end " +
                    " WHEN FCMPORD4.ANDOR4 = 2 THEN case when FCMCIAAP.codtco=189 then null when FCMCIAAP.codtco=190 then 'S' when FCMCIAAP.codtco=362 then null when FCMCIAAP.codtco=257 then 'S' else 'S' end " +
                    " WHEN FCMPORD4.ANDOR4 = 3 THEN case when FCMCIAAP.codtco=189 then null when FCMCIAAP.codtco=190 then 'S' when FCMCIAAP.codtco=362 then null when FCMCIAAP.codtco=257 then 'S' else 'S' end " +
                    " End ";
            VQuery3 = @"CASE WHEN FCMPORD4.ANDOR4 = 0 THEN case when FCMCIAAP.codtco = 189 then null when FCMCIAAP.codtco = 190 then null when FCMCIAAP.codtco = 362 then 'S' when FCMCIAAP.codtco = 257 then 'S' else 'S' end " +
                    " WHEN FCMPORD4.ANDOR4 = 1 THEN case when FCMCIAAP.codtco=189 then null when FCMCIAAP.codtco=190 then null when FCMCIAAP.codtco=362 then 'S' when FCMCIAAP.codtco=257 then 'S' else 'S' end " +
                    " WHEN FCMPORD4.ANDOR4 = 2 THEN case when FCMCIAAP.codtco=189 then null when FCMCIAAP.codtco=190 then null when FCMCIAAP.codtco=362 then 'S' when FCMCIAAP.codtco=257 then 'S' else 'S' end " +
                    " WHEN FCMPORD4.ANDOR4 = 3 THEN case when FCMCIAAP.codtco=189 then null when FCMCIAAP.codtco=190 then null when FCMCIAAP.codtco=362 then 'S' when FCMCIAAP.codtco=257 then 'S' else 'S' end " +
            " End ";

            string QueryEliminar= "DELETE FROM TE_AprOrdenCabecera where Ccia = " + CODCIA+" And CtipoDoc = '"+ CODTDC + "' And DyearDcto = "+ ANOOR1 + " And Cdocumento = " + NROOR1;
            string QUERYFINAL =
                "SELECT  CODCIA, CODTDC, ANOOR1, DCTO, FEMOR1, CODPRV, RSRPRV, CODCPP, CIFOR1, IBROR1, IDEOR1, IGFOR1, FLEOR1, SEGOR1, IIGOR1, MPIOR1, TDAOR1, IESOR1, RT4OR1, RT2OR1, AJPOR1, AJNOR1, IGEOR1, CODMON, CODCOM, OBSOR1, FPROR4, TCAOR1, COMPRADOR, MAX(FA) AS MaxDeFA,MAX(FB) AS MaxDeFB,MAX(FC) AS MaxDeFC, FEAOR1,PPUOR1,NAMFIL,URLFIL FROM ( " +
                " select FCMPORD4.codcia, FCMPORD4.codtdc, FCMPORD4.anoor1, FCMPORD4.nroor1 as dcto, femor1, FCMPORD1.codprv, rsrprv, FCMPORD1.codcpp, FCMPORD1.cifor1, FCMPORD1.ibror1, FCMPORD1.ideor1, FCMPORD1.igfor1, FCMPORD1.fleor1, FCMPORD1.segor1, FCMPORD1.iigor1, FCMPORD1.mpior1, FCMPORD1.tdaor1, FCMPORD1.iesor1, FCMPORD1.rt4or1, FCMPORD1.rt2or1, FCMPORD1.ajpor1, FCMPORD1.ajnor1,  igeor1, codmon, codcom, obsor1, " +
                " case when fpror4=0 then 0 else -1 end as fpror4, tcaor1, " +
                " ifnull((select nocper from FPLPERSO where codcia=FCMPORD1.ctror1 and tprttr=FCMPORD1.tprttr and codper= FCMPORD1.codper), (select nocpau from FPLPEAUX where codcia=FCMPORD1.ctror1 and tprpau=FCMPORD1.tprttr and codpau= FCMPORD1.codper)) as comprador, " +
                " CASE WHEN (SELECT VALTCO FROM TABCONTR WHERE CODTCO=188)=0 then " +
                " 'S' ELSE " + VQuery1 +
                " END AS FA, " +
                " CASE WHEN (SELECT VALTCO FROM TABCONTR WHERE CODTCO=188)=0 then " +
                " 'S' ELSE " + VQuery2 +
                " END AS FB, " +
                " CASE WHEN (SELECT VALTCO FROM TABCONTR WHERE CODTCO=188)=0 then " +
                " 'S' ELSE " + VQuery3 +
                " END AS FC, feaor1 , FCMPORD1.PPUOR1, FCMPORD1.NAMFIL, FCMPORD1.URLFIL  " +
                " from FCMPORD4, FCMPORD1, FABPROVE, FCMCIAAP " +
                " where FCMPORD4.codcia=FCMCIAAP.codcia and ((SA1OR4 = 0 Or SA1OR4 = 2) or (SA2OR4 = 0 Or SA2OR4 = 2) or (SA3OR4 = 0 Or SA3OR4 = 2)) and FCMPORD4.codcia=FCMPORD1.codcia and FCMPORD4.codtdc=FCMPORD1.codtdc and FCMPORD4.anoor1=FCMPORD1.anoor1 and FCMPORD4.nroor1=FCMPORD1.nroor1 and " +
                " FCMPORD1.codprv=FABPROVE.codprv and staor4<>9 and st1or1<>9 and st2or1<>9 and FCMPORD4.uvbor4 is not null and " +
                " FCMPORD1.codcia=" + CODCIA + " and FCMPORD1.codtdc='" + CODTDC + "' and FCMPORD1.anoor1=" + ANOOR1 + " and FCMPORD1.nroor1=" + NROOR1 +
                " ) A GROUP BY  CODCIA, CODTDC, ANOOR1, DCTO, FEMOR1, CODPRV, RSRPRV, CODCPP, CIFOR1, IBROR1, IDEOR1, IGFOR1, FLEOR1, SEGOR1, IIGOR1, MPIOR1, TDAOR1, IESOR1, RT4OR1, RT2OR1, AJPOR1, AJNOR1, IGEOR1, CODMON, CODCOM, OBSOR1, FPROR4, TCAOR1, COMPRADOR, FEAOR1,PPUOR1,NAMFIL,URLFIL";
           tabla = obj_DA.ObtenerDatosTE_AprOrdenCabecera(QUERYFINAL);

            int CODCIA2 = Convert.ToInt32(tabla.Rows[0]["CODCIA"].ToString());
            string CODTDC2 = tabla.Rows[0]["CODTDC"].ToString();
            int ANOOR12 = Convert.ToInt32(tabla.Rows[0]["ANOOR1"]);
            string DCTO = tabla.Rows[0]["DCTO"].ToString();
            DateTime FEMOR1 = Convert.ToDateTime(tabla.Rows[0]["FEMOR1"]);
            string CODPRV = tabla.Rows[0]["CODPRV"].ToString();
            string RSRPRV = tabla.Rows[0]["RSRPRV"].ToString();
            string CODCPP = tabla.Rows[0]["CODCPP"].ToString();
            string CIFOR1 = tabla.Rows[0]["CIFOR1"].ToString();
            string IBROR1 = tabla.Rows[0]["IBROR1"].ToString();
            string IDEOR1 = tabla.Rows[0]["IDEOR1"].ToString();
            string IGFOR1 = tabla.Rows[0]["IGFOR1"].ToString();
            string FLEOR1 = tabla.Rows[0]["FLEOR1"].ToString();
            string SEGOR1 = tabla.Rows[0]["SEGOR1"].ToString();
            string IIGOR1 = tabla.Rows[0]["IIGOR1"].ToString();
            string MPIOR1 = tabla.Rows[0]["MPIOR1"].ToString();
            string TDAOR1 = tabla.Rows[0]["TDAOR1"].ToString();
            string IESOR1 = tabla.Rows[0]["IESOR1"].ToString();
            string RT4OR1 = tabla.Rows[0]["RT4OR1"].ToString();
            string RT2OR1 = tabla.Rows[0]["RT2OR1"].ToString();
            string AJPOR1 = tabla.Rows[0]["AJPOR1"].ToString();
            string AJNOR1 = tabla.Rows[0]["AJNOR1"].ToString();
            string IGEOR1 = tabla.Rows[0]["IGEOR1"].ToString();
            string CODMON = tabla.Rows[0]["CODMON"].ToString();
            string CODCOM = tabla.Rows[0]["CODCOM"].ToString();
            string OBSOR1 = tabla.Rows[0]["OBSOR1"].ToString();
            string FPROR4 = tabla.Rows[0]["FPROR4"].ToString();
            string TCAOR1 = tabla.Rows[0]["TCAOR1"].ToString();
            string COMPRADOR = tabla.Rows[0]["COMPRADOR"].ToString();
            string MaxDeFA = tabla.Rows[0]["MaxDeFA"].ToString();
            string MaxDeFB = tabla.Rows[0]["MaxDeFB"].ToString();
            string MaxDeFC = tabla.Rows[0]["MaxDeFC"].ToString();
            DateTime FEAOR1 = Convert.ToDateTime(tabla.Rows[0]["FEAOR1"]);
            int PPUOR1 = Convert.ToInt32(tabla.Rows[0]["PPUOR1"].ToString());
            string NAMFIL = tabla.Rows[0]["NAMFIL"].ToString();
            string URLFIL = tabla.Rows[0]["URLFIL"].ToString();



            string res= obj_DA.Insertar_TE_AprOrdenCabecera(CODCIA2, CODTDC2, ANOOR12, DCTO, FEMOR1, CODPRV, RSRPRV, CODCPP, CIFOR1, IBROR1, IDEOR1, IGFOR1, FLEOR1, SEGOR1, IIGOR1, MPIOR1, TDAOR1, IESOR1, RT4OR1, RT2OR1, AJPOR1, AJNOR1, IGEOR1, CODMON, CODCOM, OBSOR1, FPROR4, TCAOR1, COMPRADOR, MaxDeFA, MaxDeFB, MaxDeFC, FEAOR1, PPUOR1, NAMFIL, URLFIL, QueryEliminar);
            return res;
        }

        
        public void TE_AprOrdenDetalleOC(int CODCIA, string CODTDC, int ANOOR1, int NROOR1)
        {
            DataTable tabla = new DataTable();
            string QueryEliminar = "DELETE FROM TE_AprOrdenDetalleOC where Ccia = " + CODCIA + " And CtipoDoc = '" + CODTDC + "' And DyearDcto = " + ANOOR1 + " And Cdocumento = " + NROOR1;

            string Query = "SELECT FCMPORD2.CODCIA, FCMPORD2.CODTDC, FCMPORD2.ANOOR1, FCMPORD2.NROOR1, FCMPORD2.CSCOR2, FCMPORD2.CANOR2, FCMPORD2.PUNOR2, FCMPORD2.PD1OR2, FCMPORD2.PD2OR2, FCMPREQ2.CODEXI, " +
                   "CASE FCMPREQ2.CODTSV WHEN 1 THEN 'NULL' END AS CODTSV, FCMPREQ1.CENCOS, IFNULL(FPLPERSO.NOCPER, FPLPEAUX.NOCPAU) AS SOLITITANTE,FMAEALGE.UNIMED, FMAEALGE.DESEXI AS EXISTENCIA, FCMPREQ1.OBSRE1 " +
                   "FROM (((((((FCMPORD2 INNER JOIN (FCMPORD1 INNER JOIN FCMPORD4 ON (FCMPORD1.NROOR1 = FCMPORD4.NROOR1) AND (FCMPORD1.ANOOR1 = FCMPORD4.ANOOR1) AND (FCMPORD1.CODTDC = FCMPORD4.CODTDC) AND (FCMPORD1.CODCIA = FCMPORD4.CODCIA)) ON (FCMPORD2.NROOR1 = FCMPORD1.NROOR1) AND (FCMPORD2.ANOOR1 = FCMPORD1.ANOOR1) AND (FCMPORD2.CODTDC = FCMPORD1.CODTDC) AND (FCMPORD2.CODCIA = FCMPORD1.CODCIA)) INNER JOIN FCMPREQ2 ON (FCMPORD2.CDAOR2 = FCMPREQ2.CSCRE2) AND (FCMPORD2.NDAOR2 = FCMPREQ2.NRORE1) AND (FCMPORD2.ADAOR2 = FCMPREQ2.ANORE1) AND (FCMPORD2.TDAOR2 = FCMPREQ2.CODTDC) AND (FCMPORD2.CODCIA = FCMPREQ2.CODCIA)) " +
                   "INNER JOIN FCMPREQ1 ON (FCMPORD2.NDAOR2 = FCMPREQ1.NRORE1) AND (FCMPORD2.ADAOR2 = FCMPREQ1.ANORE1) AND (FCMPORD2.TDAOR2 = FCMPREQ1.CODTDC) AND (FCMPORD2.CODCIA = FCMPREQ1.CODCIA)) INNER JOIN FMAEALGE ON (FCMPREQ2.CODEXI = FMAEALGE.CODEXI) AND (FCMPREQ2.CODTEX = FMAEALGE.CODTEX) AND (FCMPREQ2.CODCIA = FMAEALGE.CODCIA)) LEFT JOIN FPLPERSO ON (FCMPREQ1.CODPER = FPLPERSO.CODPER) AND (FCMPREQ1.TPRTTR = FPLPERSO.TPRTTR) AND  (FCMPREQ1.CTRRE1 = FPLPERSO.CODCIA)) LEFT JOIN FPLPEAUX ON (FCMPREQ1.CODPER = FPLPEAUX.CODPAU) AND (FCMPREQ1.TPRTTR = FPLPEAUX.TPRPAU) AND (FCMPREQ1.CTRRE1 = FPLPEAUX.CODCIA)) INNER JOIN MAESCIAS ON FCMPORD2.CODCIA = MAESCIAS.CODCIA) INNER JOIN TABTIPDC ON FCMPORD1.CODTDC = TABTIPDC.CODTDC " +
                   "WHERE (((SA1OR4 = 0 Or SA1OR4 = 2) or (SA2OR4 = 0 Or SA2OR4 = 2) or (SA3OR4 = 0 Or SA3OR4 = 2)) AND ((FCMPORD4.STAOR4)<>9) AND ((FCMPORD1.ST1OR1)<>9) AND ((FCMPORD1.ST2OR1)<>9) AND ((FCMPORD4.UVBOR4) Is Not Null) AND TABTIPDC.SISTDC = 'O' And TABTIPDC.RTATDC = 'S') " +
                   " AND FCMPORD4.CODCIA=" + CODCIA + " AND FCMPORD4.CODTDC='" + CODTDC + "' AND FCMPORD4.ANOOR1=" + ANOOR1 + " AND FCMPORD4.NROOR1=" + NROOR1;

            tabla = obj_DA.ObtenerDatosTE_AprOrdenDetalleOC(Query);
            string res = obj_DA.Insertar_TE_AprOrdenDetalleOC(tabla, QueryEliminar);


        }

        public string TE_AprOrdenDetalleOD(int CODCIA, string CODTDC, int ANOOR1, int NROOR1)
        {
            DataTable tabla = new DataTable();
            string QueryEliminar = "DELETE FROM TE_AprOrdenDetalleOD where Ccia = " + CODCIA + " And CtipoDoc = '" + CODTDC + "' And DyearDcto = " + ANOOR1 + " And Cdocumento = " + NROOR1;

            string Query = "SELECT FCMPORD3.CODCIA, FCMPORD3.CODTDC, FCMPORD3.ANOOR1, FCMPORD3.NROOR1, FCMPORD3.CSCOR3, FCMPORD3.CODTSV, FCMPORD3.CENCOS, FCMPORD3.DE1OR3, FCMPORD3.IMPOR3, IFNULL(FPLPERSO.NOCPER, FPLPEAUX.NOCPAU) AS SOLICITANTE  " +
                  " FROM (((FCMPORD1 INNER JOIN FCMPORD4 ON (FCMPORD1.NROOR1 = FCMPORD4.NROOR1) AND (FCMPORD1.ANOOR1 = FCMPORD4.ANOOR1) AND (FCMPORD1.CODTDC = FCMPORD4.CODTDC) AND (FCMPORD1.CODCIA = FCMPORD4.CODCIA)) INNER JOIN FCMPORD3 ON (FCMPORD4.NROOR1 = FCMPORD3.NROOR1) AND (FCMPORD4.ANOOR1 = FCMPORD3.ANOOR1) AND (FCMPORD4.CODTDC = FCMPORD3.CODTDC) AND (FCMPORD4.CODCIA = FCMPORD3.CODCIA)) LEFT JOIN FPLPERSO ON (FCMPORD1.CODPER = FPLPERSO.CODPER) AND (FCMPORD1.TPRTTR = FPLPERSO.TPRTTR) AND (FCMPORD1.CTROR1 = FPLPERSO.CODCIA)) LEFT JOIN FPLPEAUX ON (FCMPORD1.CODPER = FPLPEAUX.CODPAU) AND (FCMPORD1.TPRTTR = FPLPEAUX.TPRPAU) AND (FCMPORD1.CTROR1 = FPLPEAUX.CODCIA)  INNER JOIN TABTIPDC ON FCMPORD1.CODTDC = TABTIPDC.CODTDC " +
                  " WHERE (TABTIPDC.SISTDC='O' AND TABTIPDC.RTATDC='N') AND ((SA1OR4 = 0 Or SA1OR4 = 2) or (SA2OR4 = 0 Or SA2OR4 = 2) or (SA3OR4 = 0 Or SA3OR4 = 2)) AND FCMPORD4.STAOR4<>9 AND FCMPORD1.ST1OR1<>9 AND FCMPORD1.ST2OR1<>9 AND FCMPORD4.UVBOR4 Is Not Null " +
                  " AND FCMPORD4.CODCIA=" + CODCIA + " AND FCMPORD4.CODTDC='" + CODTDC + "' AND FCMPORD4.ANOOR1=" + ANOOR1 + " AND FCMPORD4.NROOR1=" + NROOR1;


            tabla = obj_DA.ObtenerDatosTE_AprOrdenDetalleOD(Query);

            string resultadoTotal = "";
            for (int i = 0; i < tabla.Rows.Count; i++)
            {
                int CODCIA2 = Convert.ToInt32(tabla.Rows[i]["CODCIA"]);
                string CODTDC2 = tabla.Rows[i]["CODTDC"].ToString();
                int ANOOR12 = Convert.ToInt32(tabla.Rows[i]["ANOOR1"].ToString());
                string NROOR12 = tabla.Rows[i]["NROOR1"].ToString();
                string CSCOR3 = tabla.Rows[i]["CSCOR3"].ToString();
                string CODTSV = tabla.Rows[i]["CODTSV"].ToString();
                string CENCOS = tabla.Rows[i]["CENCOS"].ToString();
                string DE1OR3 = tabla.Rows[i]["DE1OR3"].ToString();
                decimal IMPOR3 =Convert.ToDecimal(tabla.Rows[i]["IMPOR3"].ToString());
                string SOLICITANTE = tabla.Rows[i]["SOLICITANTE"].ToString();

                string res = obj_DA.Insertar_TE_AprOrdenDetalleOD(CODCIA2, CODTDC2, ANOOR12, NROOR12, CSCOR3, CODTSV, CENCOS, DE1OR3, IMPOR3, SOLICITANTE, QueryEliminar);
                resultadoTotal += res + "\n";
            }

            return resultadoTotal;

        }
        
        public string T_OperaAprOrden(int CODCIA, string CODTDC, int ANOOR1, int NROOR1)
        {
            DataTable tabla = new DataTable();
            string QueryEliminar = "DELETE FROM T_OperaAprOrden where Ccia = " + CODCIA + " And CtipoDoc = '" + CODTDC + "' And DyearDcto = " + ANOOR1 + " And Cdocumento = " + NROOR1;

            string Query = "SELECT FCMPORD4.CODCIA, FCMPORD4.CODTDC, FCMPORD4.ANOOR1, FCMPORD4.NROOR1, FCMPORD4.SECOR4, FCMPORD4.FMOOR4, FCMPORD4.FPROR4, FCMPORD4.OUROR4, FCMPORD4.FVBOR4, FCMPORD4.UVBOR4, FCMPORD4.FA1OR4, FCMPORD4.UA1OR4, FCMPORD4.FD1OR4, FCMPORD4.OD1OR4, FCMPORD4.SA1OR4, FCMPORD4.FA2OR4, FCMPORD4.UA2OR4, FCMPORD4.FD2OR4, FCMPORD4.OD2OR4, FCMPORD4.SA2OR4, FCMPORD4.FA3OR4, FCMPORD4.UA3OR4, FCMPORD4.FD3OR4, FCMPORD4.OD3OR4, FCMPORD4.SA3OR4, FCMPORD4.FHROR4, FCMPORD4.STAOR4 " +
                  " FROM FCMPORD4 INNER JOIN FCMPORD1 ON (FCMPORD4.CODCIA = FCMPORD1.CODCIA) AND (FCMPORD4.CODTDC = FCMPORD1.CODTDC) AND (FCMPORD4.ANOOR1 = FCMPORD1.ANOOR1) AND (FCMPORD4.NROOR1 = FCMPORD1.NROOR1) " +
                  " WHERE FCMPORD4.UVBOR4 Is Not Null And FCMPORD4.STAOR4 <> 9 And ((SA1OR4 = 0 Or SA1OR4 = 2) or (SA2OR4 = 0 Or SA2OR4 = 2) or (SA3OR4 = 0 Or SA3OR4 = 2)) And FCMPORD1.ST1OR1 <> 9 And FCMPORD1.ST2OR1 <> 9 " +
                  " AND FCMPORD4.CODCIA=" + CODCIA + " AND FCMPORD4.CODTDC='" + CODTDC + "' AND FCMPORD4.ANOOR1=" + ANOOR1 + " AND FCMPORD4.NROOR1=" + NROOR1;


            tabla = obj_DA.ObtenerDatos_T_OperaAprOrden(Query);
            int CODCIA2 = Convert.ToInt32(tabla.Rows[0]["CODCIA"].ToString());
            string CODTDC2 = tabla.Rows[0]["CODTDC"].ToString();
            string ANOOR12 = tabla.Rows[0]["ANOOR1"].ToString();
            string NROOR12 = tabla.Rows[0]["NROOR1"].ToString();
            string SECOR4 = tabla.Rows[0]["SECOR4"].ToString();
            string FMOOR4 = tabla.Rows[0]["FMOOR4"].ToString();
            string FPROR4 = tabla.Rows[0]["FPROR4"].ToString();
            string OUROR4 = tabla.Rows[0]["OUROR4"].ToString();
            string FVBOR4 = tabla.Rows[0]["FVBOR4"].ToString();
            string UVBOR4 = tabla.Rows[0]["UVBOR4"].ToString();
            string FA1OR4 = tabla.Rows[0]["FA1OR4"].ToString();
            string UA1OR4 = tabla.Rows[0]["UA1OR4"].ToString();
            string FD1OR4 = tabla.Rows[0]["FD1OR4"].ToString();
            string OD1OR4 = tabla.Rows[0]["OD1OR4"].ToString();
            string SA1OR4 = tabla.Rows[0]["SA1OR4"].ToString();
            string FA2OR4 = tabla.Rows[0]["FA2OR4"].ToString();
            string UA2OR4 = tabla.Rows[0]["UA2OR4"].ToString();
            string FD2OR4 = tabla.Rows[0]["FD2OR4"].ToString();
            string OD2OR4 = tabla.Rows[0]["OD2OR4"].ToString();
            string SA2OR4 = tabla.Rows[0]["SA2OR4"].ToString();
            string FA3OR4 = tabla.Rows[0]["FA3OR4"].ToString();
            string UA3OR4 = tabla.Rows[0]["UA3OR4"].ToString();
            string FD3OR4 = tabla.Rows[0]["FD3OR4"].ToString();
            string OD3OR4 = tabla.Rows[0]["OD3OR4"].ToString();
            string SA3OR4 = tabla.Rows[0]["SA3OR4"].ToString();
            string FHROR4 = tabla.Rows[0]["FHROR4"].ToString();
            string STAOR4 = tabla.Rows[0]["STAOR4"].ToString();

            string res = obj_DA.Insertar_T_OperaAprOrden(CODCIA2, CODTDC2, ANOOR12, NROOR12, SECOR4, FMOOR4, FPROR4, OUROR4, FVBOR4, UVBOR4,
                FA1OR4, UA1OR4, FD1OR4, OD1OR4, SA1OR4, FA2OR4, UA2OR4, FD2OR4, OD2OR4, SA2OR4, FA3OR4, UA3OR4, FD3OR4, OD3OR4, SA3OR4, FHROR4, STAOR4, QueryEliminar);
            return res;

        }
        
            public void CambiaEstadoOrden(int CODCIA, string CODTDC, int ANOOR1, int NROOR1)
        {
            
            DataTable tabla = new DataTable();
            string Query = " UPDATE FCMPORD1 SET STROR1 = 0 where CODCIA = " + CODCIA + " And CODTDC = '" + CODTDC + "' And ANOOR1 = " + ANOOR1 + " And NROOR1 = " + NROOR1;
             obj_DA.CambiaEstadoOrden(Query);

        }
    }
}

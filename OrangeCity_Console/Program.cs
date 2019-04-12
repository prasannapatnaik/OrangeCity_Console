using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace OrangeCity_Console
{
    class Program
    {
        public static List<string> setup = new List<string>();
        public static List<string> TotDate = new List<string>();
        public static List<string> locs = new List<string>();
        public static string LocalDB,LocalNgpDB, line, path, typeprocess, Location, TotalFlowUnitMk, FlowUnitMk, RemoteDB, LogQuery, Key, SNO, logpath = "";
        public static Int32 LogSerialNo, LogSIMNo;
        public static SqlConnection LocalDBconn = new SqlConnection();
        public static SqlConnection NGPDBconn = new SqlConnection();
        public static SqlConnection LocalNgpDBconn = new SqlConnection();
        public static StreamWriter sw;
        //public static string[] allfiles;
        public static Int32 i, j = 1;
        public static decimal FlowRate, ActFlowRate, Consumption, Lat, Long, PressureValue = 0;
        public static double FlowUnit, TotalFlowUnit, LastPeriod;
        //public static ConsoleKeyInfo cki;
        public static DateTime From_Date, To_Date;
        ;

        static void Main(string[] args)
        {
            path = "C:\\HWM\\SCADA\\NGP.txt";

            if (!File.Exists(path))
            {
                string[] lines = { "Title-", "Server-", "Database-", "UserName-", "Password-", "Local-", "Remote-", "Error-", "Mode (A/M)-", "Totalizer Units (m3/Lts)-", "Flow Rate (Lts.s/m3.h)-", "Totalizer Correction-", "Flow rate Correction-", "Interval (hrs)-", "Interval (Mins)-", "Channel No (Flow)-", "License No-", "License Status (Y/N/NA)-NA", "Transfer (FTP/Local)-", "FTP IP-", "FTP/WIndows Username-", "FTP/Windows Password-", "Delete (Y/N)-", "Local 2-" };

                System.IO.File.WriteAllLines(path, lines);
            }

            System.IO.StreamReader file =
              new StreamReader(path);
            while ((line = file.ReadLine()) != null)
            {
                int index = line.LastIndexOf("-");
                if (index > 0)
                    line = line.Substring(index + 1);
                if (!String.IsNullOrWhiteSpace(line))
                {
                    setup.Add(line);
                }
                else
                { j = 0; }

                //call your function here  
            }
            if (j != 0)
            {
                //string filename = String.Format("{0:yyyy-MM-dd}__{1}", DateTime.Now, "Log.txt");
                //string path = Path.Combine(setup[27], filename);
                ////logpath = setup[27] + "\\" + filename;
                //sw = new StreamWriter(path);

                //fs = new FileStream(path, System.IO.FileMode.Create);

                LocalDB = "Data Source=" + setup[1] + ";MultipleActiveResultSets=true;Initial Catalog=" + setup[2] + ";Persist Security Info=True;User ID=" + setup[3] + ";Password=" + setup[4] + "";
                Console.Title = setup[0];
                RemoteDB = "Data Source=" + setup[5] + ";MultipleActiveResultSets=true;Initial Catalog=" + setup[6] + ";Persist Security Info=True;User ID=" + setup[7] + ";Password=" + setup[8] + "";
                LocalNgpDB = "Data Source=" + setup[1] + ";MultipleActiveResultSets=true;Initial Catalog=" + setup[19] + ";Persist Security Info=True;User ID=" + setup[3] + ";Password=" + setup[4] + "";
                StartProcess();
            }
        }

        private static void StartProcess()
        {
            if (setup[8] == "A")
            {
                typeprocess = "A";
                From_Date = DateTime.Today;
                To_Date = DateTime.Today.AddDays(-1);
                Console.WriteLine("Trigger Auto from " + From_Date + " To " + To_Date);
                Console.WriteLine("--------------------------------------------------");
                //System.IO.StreamWriter sw = new System.IO.StreamWriter(fs);
                //System.Console.SetOut(sw);
                //sw.WriteLine("Trigger Auto from " + From_Date + " To " + To_Date);

            }
            else if (setup[8] == "M")
            {
                typeprocess = "M";
            }
            Object FromDate = From_Date;
            Object ToDate = To_Date;
            Object Type = typeprocess;
            
            starttoconvert(From_Date, To_Date, typeprocess);
        }

        private static void starttoconvert(DateTime from_Date, DateTime to_Date, string typeprocess)
        {
            Int32 Hours = Convert.ToInt32(setup[14]);
            Int32 Mins = Convert.ToInt32(setup[15]);
            Int32 FChannel = Convert.ToInt32(setup[16]);
            Int32 PChannel = Convert.ToInt32(setup[17]);
            Int32 InsertType = Convert.ToInt32(setup[18]);

            try
            {

                string query = "SELECT MIN(MeterRead) AS MINIMUM, MAX(MeterRead) AS MAXIMUM, AVG(Consumption) FROM dataExportEwp";
                string query_flow = "SELECT MAX (datapoints.value) FROM datapoints INNER JOIN sites ON datapoints.SiteID = sites.ID WHERE(datapoints.ChannelNumber = " + FChannel + ")";
                string disQuery = "SELECT DISTINCT Mprn FROM dataExportEwp";
                string pressurequery = "SELECT loggers.LoggerSerialNumber, sites.SiteID, loggers.LoggerSMSNumber, loggers.MastLongitude, loggers.MastLatitude, datapoints.DataTime, datapoints.ChannelNumber, datapoints.value FROM sites INNER JOIN loggers ON sites.LoggerID = loggers.ID INNER JOIN datapoints ON sites.LoggerID = datapoints.SiteID";

                LocalDBconn = new SqlConnection(LocalDB);
                //bwssbcon = new SqlConnection(connectionstring);

                LocalDBconn.Open();
                //Get Distinct Location Names
                SqlCommand QueryChk = new SqlCommand(disQuery, LocalDBconn);
                List<LocationsID> results = new List<LocationsID>();
                using (SqlDataReader oReader = QueryChk.ExecuteReader())
                {
                    while (oReader.Read())
                    {
                        LocationsID Loc = new LocationsID();
                        Loc.LocID = oReader[0].ToString();
                        results.Add(Loc);
                    }
                }
                //txt_locations.Text = results.Count.ToString();
                LocalDBconn.Close();
                List<DateTime> dates = new List<DateTime>();

                for (var dt = from_Date; dt < to_Date; dt = dt.AddHours(Hours).AddMinutes(Mins))
                {
                    dates.Add(dt);
                }

                for (i = 0; i < results.Count; i++)
                {
                    for (int j = 0; j < dates.Count - 1; j++)
                    {
                        try
                        {
                            LocalDBconn.Open();
                            string from = dates[j].AddSeconds(1).ToString("M/d/yyyy HH:mm:ss");
                            string to = dates[j + 1].ToString("M/d/yyyy HH:mm:ss");
                            string prev = dates[j].AddHours(-Hours).AddMinutes(-Mins).AddSeconds(1).ToString("M/d/yyyy HH:mm:ss");

                            Location = results[i].LocID.Substring(0, results[i].LocID.IndexOf('_'));

                            String query2_CurrentTotalizer = query + " Where Mprn = '" + results[i].LocID + "' AND datetime >='" + from + "' AND datetime<='" + to + "'";
                            String query2_CurrentFlow = query_flow + " AND (datapoints.DataTime >= '" + from + "') AND (datapoints.DataTime <= '" + to + "') AND (sites.SiteID = '" + Location + "')";
                            String query2_PrevTotalizer = query + " Where Mprn = '" + results[i].LocID + "' AND datetime >='" + prev + "' AND datetime<'" + from + "'";
                            string query2_Pressure = pressurequery + " where sites.SiteID = '" + Location + "' AND datapoints.ChannelNumber= " + PChannel + " AND datetime >='" + from + "' AND datetime<='" + to + "'";

                            SqlCommand CurrentTot = new SqlCommand(query2_CurrentTotalizer, LocalDBconn);
                            //Console.WriteLine(query2);
                            SqlCommand QueryFlow = new SqlCommand(query2_CurrentFlow, LocalDBconn);

                            SqlCommand PrevTot = new SqlCommand(query2_PrevTotalizer, LocalDBconn);

                            SqlCommand Pressure = new SqlCommand(query2_Pressure, LocalDBconn);
                            //Console.WriteLine(query2_flow);

                            using (SqlDataReader oReader2 = CurrentTot.ExecuteReader())
                            {
                                while (oReader2.Read())
                                {
                                    string matter = "";

                                    TotalFlowUnit = 1;
                                    TotalFlowUnitMk = "1";
                                    if (setup[9] == "m3") { TotalFlowUnit = 1; TotalFlowUnitMk = "1"; }
                                    else if (setup[9] == "Lts") { TotalFlowUnit = 1000; TotalFlowUnitMk = "0"; }

                                    Double CurrentPeriod = 0;
                                    if (String.IsNullOrEmpty(oReader2[1].ToString()))
                                    {
                                        CurrentPeriod = 0;
                                    }
                                    else
                                    {
                                        CurrentPeriod = Convert.ToDouble(oReader2[1].ToString());
                                        if (setup[11] != "1")
                                        {
                                            CurrentPeriod = CurrentPeriod * Convert.ToDouble(setup[11]) * TotalFlowUnit;
                                        }
                                    }

                                    //Previous Flow
                                    using (SqlDataReader oReader4 = PrevTot.ExecuteReader())
                                    {
                                        while (oReader4.Read())
                                        {
                                            TotalFlowUnit = 1;
                                            TotalFlowUnitMk = "1";
                                            if (setup[9] == "m3") { TotalFlowUnit = 1; TotalFlowUnitMk = "1"; }
                                            else if (setup[9] == "Lts") { TotalFlowUnit = 1000; TotalFlowUnitMk = "0"; }

                                            LastPeriod = 0;
                                            if (String.IsNullOrEmpty(oReader4[0].ToString()))
                                            {
                                                LastPeriod = 0;
                                            }
                                            else
                                            {
                                                LastPeriod = Convert.ToDouble(oReader4[0].ToString());
                                                if (setup[11] != "1")
                                                {
                                                    LastPeriod = LastPeriod * Convert.ToDouble(setup[11]) * TotalFlowUnit;
                                                }
                                            }

                                        }
                                    }

                                    //Pressure/Serial No/Lat/Long
                                    using (SqlDataReader oReader5 = Pressure.ExecuteReader())
                                    {
                                        while (oReader5.Read())
                                        {
                                            LogSerialNo = Convert.ToInt32(oReader5[0].ToString());
                                            LogSIMNo = Convert.ToInt32(oReader5[2].ToString());
                                            
                                            if (String.IsNullOrEmpty(oReader5[3].ToString()))
                                            {
                                                Long = 0;
                                            }
                                            else
                                            {
                                                Long = Convert.ToDecimal(oReader5[3].ToString());
                                            }

                                            if (String.IsNullOrEmpty(oReader5[4].ToString()))
                                            {
                                                Lat = 0;
                                            }
                                            else
                                            {
                                                Lat = Convert.ToDecimal(oReader5[4].ToString());
                                            }

                                            if (String.IsNullOrEmpty(oReader5[7].ToString()))
                                            {
                                                PressureValue = 0;
                                            }
                                            else
                                            {
                                                PressureValue = Convert.ToDecimal(oReader5[7].ToString());
                                            }

                                        }
                                    }

                                    //Flow Rate
                                    using (SqlDataReader oReader3 = QueryFlow.ExecuteReader())
                                    {
                                        while (oReader3.Read())
                                        {
                                            FlowUnit = 1;
                                            ActFlowRate = 0;
                                            FlowRate = 0;
                                            
                                            Consumption = Convert.ToDecimal(CurrentPeriod - LastPeriod);
                                            Consumption = Consumption / Convert.ToDecimal(setup[11]);
                                            if (Consumption != 0)
                                            {
                                                decimal hrs = Hours * 60;
                                                hrs = hrs + Mins;
                                                if (setup[10] == "m3.h") { hrs = hrs / 60; }


                                                FlowRate = Consumption / hrs;
                                                FlowRate = FlowRate * Convert.ToDecimal(setup[12]);
                                            }
                                            ActFlowRate = Math.Round(FlowRate, 2);
                                        }
                                    }

                                    Console.WriteLine("--------------------------------------------------");

                                    
                                }
                            }
                            LocalDBconn.Close();
                        }
                        catch (Exception Ex)
                        {
                            Console.WriteLine("Exception Occurred :{0},{1}", Ex.Message, Ex.StackTrace.ToString());
                            // Console.ReadLine();
                        }

                    }

                    LocalDBconn.Close();
                }  
            }
            catch (Exception Ex)
            { }
        }
    }
}
    

       
